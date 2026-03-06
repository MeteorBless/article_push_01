# -*- coding: utf-8 -*-
"""
digest_generate_simple_list.py

功能：
1) 从 news.db 里联表查询：articles_deduped（去重主条） + article_topics（single主题）
2) （可选）调用大模型为每条新闻生成“一句话日报摘要”
3) 生成简洁的 Markdown 日报：仅编号列表
   形如：1. **标题**：一句话摘要
4) 写入 news.db 推送相关表：
   - digest_runs / digest_items / digest_topic_stats / digest_sent（可选：标记已推送）
5) 将 Markdown 文件写入 repoter/ 目录（可用 --output-dir 指定）

依赖：
pip install requests
"""

import argparse
import datetime
import json
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# 配置（按你前面文件口径）
# =========================
DB_PATH = "news.db"

# 硅基流动 / OpenAI兼容 ChatCompletions
LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"

# 你说“API直接写在文件里就好”——这里留占位。你可直接改成你的 key。
SILICONFLOW_API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"

TEMPERATURE = 0.0
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 0.4

# LLM 每批处理多少条（避免上下文过长）
LLM_BATCH_SIZE = 20

# 摘要长度控制
SUMMARY_MAX_CHARS = 60

# 默认输出目录（按你要求：repoter）
DEFAULT_OUTPUT_DIR = "repoter"

# =========================
# 推送表（写入 news.db）
# =========================
DIGEST_RUNS = "digest_runs"
DIGEST_ITEMS = "digest_items"
DIGEST_TOPIC_STATS = "digest_topic_stats"
DIGEST_SENT = "digest_sent"

# =========================


def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def day_str(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def ensure_digest_tables(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIGEST_RUNS} (
      run_date        TEXT PRIMARY KEY,
      created_at      TEXT NOT NULL,
      source_run_date TEXT NOT NULL,
      total_candidates INTEGER NOT NULL DEFAULT 0,
      total_selected   INTEGER NOT NULL DEFAULT 0,
      status          TEXT NOT NULL DEFAULT 'draft',
      channel         TEXT,
      send_at         TEXT,
      send_error      TEXT,
      digest_title    TEXT,
      digest_text     TEXT
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DIGEST_RUNS}_status ON {DIGEST_RUNS}(status);")

    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIGEST_ITEMS} (
      run_date      TEXT NOT NULL,
      item_no       INTEGER NOT NULL,

      topic         TEXT NOT NULL,
      topic_confidence REAL,
      topic_method  TEXT,
      topic_reason  TEXT,

      article_id    INTEGER NOT NULL,
      dedup_group_id TEXT,
      decision      TEXT,

      pub_time      TEXT,
      source        TEXT,
      title         TEXT NOT NULL,
      url           TEXT,

      summary       TEXT,
      score         REAL,
      picked_reason TEXT,
      created_at    TEXT NOT NULL,

      PRIMARY KEY (run_date, item_no),
      UNIQUE (run_date, article_id)
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DIGEST_ITEMS}_run_topic ON {DIGEST_ITEMS}(run_date, topic);")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DIGEST_ITEMS}_article ON {DIGEST_ITEMS}(article_id);")

    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIGEST_TOPIC_STATS} (
      run_date   TEXT NOT NULL,
      topic      TEXT NOT NULL,
      total_cnt  INTEGER NOT NULL,
      shown_cnt  INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_date, topic)
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DIGEST_TOPIC_STATS}_run ON {DIGEST_TOPIC_STATS}(run_date);")

    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {DIGEST_SENT} (
      article_id INTEGER PRIMARY KEY,
      first_sent_date TEXT NOT NULL,
      last_sent_date  TEXT NOT NULL,
      times_sent      INTEGER NOT NULL DEFAULT 1,
      created_at      TEXT NOT NULL
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{DIGEST_SENT}_last ON {DIGEST_SENT}(last_sent_date);")

    conn.commit()


def delete_digest_for_day(conn: sqlite3.Connection, run_date: str) -> None:
    conn.execute(f"DELETE FROM {DIGEST_ITEMS} WHERE run_date = ?", (run_date,))
    conn.execute(f"DELETE FROM {DIGEST_TOPIC_STATS} WHERE run_date = ?", (run_date,))
    conn.execute(f"DELETE FROM {DIGEST_RUNS} WHERE run_date = ?", (run_date,))
    conn.commit()


def fetch_candidates(conn: sqlite3.Connection, run_date: str) -> List[Dict[str, Any]]:
    """
    联表：topic + dedup
    并排除 digest_sent 里已推送过的 article_id（推送层防重）
    """
    rows = conn.execute(
        f"""
        SELECT
          t.topic,
          t.confidence,
          t.method,
          t.reason,

          d.article_id,
          COALESCE(d.title,''),
          COALESCE(d.source,''),
          COALESCE(d.pub_time,''),
          COALESCE(d.url,''),
          COALESCE(d.content_text,''),
          COALESCE(d.dedup_group_id,''),
          COALESCE(d.decision,'')
        FROM article_topics t
        JOIN articles_deduped d
          ON t.article_id = d.article_id
         AND t.run_date   = d.dedup_run_date
        LEFT JOIN {DIGEST_SENT} s
          ON s.article_id = d.article_id
        WHERE t.run_date = ?
          AND s.article_id IS NULL
        ORDER BY d.pub_time DESC, d.article_id DESC
        """,
        (run_date,)
    ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "topic": r[0],
            "topic_confidence": r[1],
            "topic_method": r[2],
            "topic_reason": r[3],

            "article_id": int(r[4]),
            "title": clean_text(r[5]),
            "source": clean_text(r[6]),
            "pub_time": r[7],
            "url": r[8],
            "content_text": r[9] or "",
            "dedup_group_id": r[10],
            "decision": r[11],
        })
    return out


def simple_score(it: Dict[str, Any]) -> float:
    """
    轻量排序：默认按信息量 + 轻微偏向权威来源。
    """
    title = it.get("title", "")
    content = it.get("content_text", "")
    source = it.get("source", "")

    score = 0.0
    score += min(len(title), 50) / 50.0
    score += min(len(content), 2000) / 2000.0

    # 权威来源轻微加分（你可自行扩充）
    authority = ["新华社", "人民日报", "求是", "央视", "中央广播电视总台", "新华网", "人民网"]
    if any(a in source for a in authority):
        score += 0.2

    return float(score)


def pick_items(candidates: List[Dict[str, Any]], total_limit: int) -> List[Dict[str, Any]]:
    """
    只输出“编号列表”时，不需要分 topic 展示。
    这里采用：score + 时间倒序 的混合排序。
    """
    # 先算分
    for it in candidates:
        it["score"] = simple_score(it)

    # pub_time 可能为空，按字符串排序时空会排前；这里做兜底：空当成最小
    def sort_key(x: Dict[str, Any]) -> Tuple[float, str, int]:
        return (x.get("score", 0.0), x.get("pub_time") or "", x.get("article_id", 0))

    candidates_sorted = sorted(candidates, key=sort_key, reverse=True)
    return candidates_sorted[:total_limit]


def build_llm_prompt(items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    system = "你是新闻日报编辑。你必须只输出严格JSON，不能输出任何额外文字或markdown。"
    payload = {
        "task": "daily_digest_one_sentence",
        "requirements": [
            "为每条新闻生成一句话摘要，写成适合日报速览的风格。",
            f"每条摘要不超过{SUMMARY_MAX_CHARS}个汉字左右，尽量包含主体+动作+亮点/影响。",
            "不要使用编号，不要换行，不要出现多余引号。",
            "如标题已很完整，摘要可补充关键特性/数据/意义。",
        ],
        "output_format": {
            "items": [
                {"article_id": "int", "summary": "string"}
            ]
        },
        "items": []
    }

    for it in items:
        # 给模型：标题 + 来源 + 时间 + 正文前若干
        content_preview = clean_text(it.get("content_text", ""))[:900]
        payload["items"].append({
            "article_id": it["article_id"],
            "title": it.get("title", ""),
            "source": it.get("source", ""),
            "pub_time": it.get("pub_time", ""),
            "url": it.get("url", ""),
            "content_preview": content_preview
        })

    user = "请按要求输出JSON：\n" + json.dumps(payload, ensure_ascii=False)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def extract_first_json(text: str) -> str:
    text = (text or "").strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError(f"无法提取JSON，输出前200字：{text[:200]}")
    return m.group(0)


def call_llm(messages: List[Dict[str, str]]) -> str:
    headers = {"Authorization": f"Bearer {SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": LLM_MODEL, "messages": messages, "temperature": TEMPERATURE}
    resp = requests.post(LLM_URL, headers=headers, json=payload, timeout=TIMEOUT_SECONDS)
    if resp.status_code != 200:
        raise RuntimeError(f"LLM 调用失败 HTTP {resp.status_code}：{resp.text[:2000]}")
    obj = resp.json()
    return obj["choices"][0]["message"]["content"]


def llm_summarize(items: List[Dict[str, Any]]) -> Dict[int, str]:
    """
    批量生成摘要，返回 article_id -> summary
    """
    if not SILICONFLOW_API_KEY or "REPLACE_ME" in SILICONFLOW_API_KEY:
        raise RuntimeError("SILICONFLOW_API_KEY 未正确配置（请在文件里填入你的 key）。")

    mapping: Dict[int, str] = {}
    for start in range(0, len(items), LLM_BATCH_SIZE):
        batch = items[start:start + LLM_BATCH_SIZE]
        messages = build_llm_prompt(batch)
        reply = call_llm(messages)
        parsed = json.loads(extract_first_json(reply))
        out_items = parsed.get("items") or []
        for it in out_items:
            try:
                aid = int(it.get("article_id"))
            except Exception:
                continue
            summary = clean_text(str(it.get("summary") or ""))
            if not summary:
                continue
            if len(summary) > SUMMARY_MAX_CHARS + 10:
                summary = summary[:SUMMARY_MAX_CHARS].rstrip("，。；;:：") + "。"
            mapping[aid] = summary
        time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)

    return mapping


def build_markdown_simple(run_date: str, items: List[Dict[str, Any]]) -> str:
    """
    只生成你要的那种编号列表，不做任何主题概览/分topic。
    """
    lines: List[str] = []
    for idx, it in enumerate(items, start=1):
        title = it.get("title", "").strip() or f"article_{it.get('article_id')}"
        summary = (it.get("summary") or "").strip()
        if summary:
            lines.append(f"{idx}. **{title}**：{summary}")
        else:
            # 没摘要时给个兜底（不加topic）
            lines.append(f"{idx}. **{title}**")
    return "\n".join(lines) + "\n"


def write_markdown_file(output_dir: str, run_date: str, md: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"daily_digest_{run_date}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    return path


def upsert_digest_run(conn: sqlite3.Connection, run_date: str, source_run_date: str,
                      total_candidates: int, total_selected: int, digest_text: str,
                      channel: Optional[str] = None) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {DIGEST_RUNS}
          (run_date, created_at, source_run_date, total_candidates, total_selected, status, channel, digest_title, digest_text)
        VALUES (?, ?, ?, ?, ?, COALESCE((SELECT status FROM {DIGEST_RUNS} WHERE run_date=?), 'draft'), ?, ?, ?)
        """,
        (run_date, now_ts(), source_run_date, int(total_candidates), int(total_selected), run_date, channel, f"每日新闻速览 {run_date}", digest_text)
    )


def insert_digest_items(conn: sqlite3.Connection, run_date: str, items: List[Dict[str, Any]]) -> None:
    for idx, it in enumerate(items, start=1):
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {DIGEST_ITEMS} (
              run_date, item_no,
              topic, topic_confidence, topic_method, topic_reason,
              article_id, dedup_group_id, decision,
              pub_time, source, title, url,
              summary, score, picked_reason,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_date, idx,
                it.get("topic") or "",
                it.get("topic_confidence", None),
                it.get("topic_method", None),
                it.get("topic_reason", None),

                int(it["article_id"]),
                it.get("dedup_group_id", ""),
                it.get("decision", ""),

                it.get("pub_time", ""),
                it.get("source", ""),
                it.get("title", ""),
                it.get("url", ""),

                it.get("summary", ""),
                it.get("score", None),
                it.get("picked_reason", None),

                now_ts()
            )
        )


def insert_topic_stats(conn: sqlite3.Connection, run_date: str,
                       total_by_topic: Dict[str, int],
                       shown_by_topic: Dict[str, int]) -> None:
    for topic, total_cnt in total_by_topic.items():
        shown_cnt = int(shown_by_topic.get(topic, 0))
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {DIGEST_TOPIC_STATS}
              (run_date, topic, total_cnt, shown_cnt, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_date, topic, int(total_cnt), shown_cnt, now_ts())
        )


def mark_sent(conn: sqlite3.Connection, run_date: str, article_ids: List[int]) -> None:
    for aid in article_ids:
        row = conn.execute(
            f"SELECT first_sent_date, last_sent_date, times_sent FROM {DIGEST_SENT} WHERE article_id=?",
            (int(aid),)
        ).fetchone()
        if row:
            first_sent, last_sent, times = row
            conn.execute(
                f"""
                UPDATE {DIGEST_SENT}
                SET last_sent_date=?, times_sent=?, created_at=?
                WHERE article_id=?
                """,
                (run_date, int(times) + 1, now_ts(), int(aid))
            )
        else:
            conn.execute(
                f"""
                INSERT INTO {DIGEST_SENT} (article_id, first_sent_date, last_sent_date, times_sent, created_at)
                VALUES (?, ?, ?, 1, ?)
                """,
                (int(aid), run_date, run_date, now_ts())
            )

    conn.execute(
        f"""
        UPDATE {DIGEST_RUNS}
        SET status='sent', send_at=?, channel=COALESCE(channel, ?)
        WHERE run_date=?
        """,
        (now_ts(), "unknown", run_date)
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, default=None, help="run_date, 格式 YYYY-MM-DD。默认：昨天")
    ap.add_argument("--total-limit", type=int, default=50, help="日报最多展示多少条（默认50）")
    ap.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Markdown输出目录（默认repoter/）")
    ap.add_argument("--overwrite", action="store_true", help="重跑：清空当日 digest_* 再写")
    ap.add_argument("--no-llm-summary", action="store_true", help="不调用LLM生成摘要（用截断正文兜底）")
    ap.add_argument("--channel", type=str, default=None, help="记录渠道（feishu/wecom/email等）")
    ap.add_argument("--mark-sent", action="store_true", help="生成后标记已推送（写 digest_sent）")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    if args.date:
        run_date = args.date.strip()
        # 简单校验
        datetime.datetime.strptime(run_date, "%Y-%m-%d")
    else:
        run_date = day_str(datetime.date.today() - datetime.timedelta(days=1))

    conn = sqlite3.connect(DB_PATH)
    ensure_digest_tables(conn)

    if args.overwrite:
        delete_digest_for_day(conn, run_date)

    candidates = fetch_candidates(conn, run_date)
    total_candidates = len(candidates)
    if total_candidates == 0:
        conn.close()
        raise RuntimeError(f"没有可用于日报的候选数据（run_date={run_date}）。请先确保 news_writeback + single 已跑完，且未被 digest_sent 全部过滤。")

    picked = pick_items(candidates, total_limit=int(args.total_limit))

    # 统计（用于落库，虽然正文不展示topic，但你之后可用于分析）
    total_by_topic: Dict[str, int] = {}
    for it in candidates:
        t = it.get("topic") or "未知"
        total_by_topic[t] = total_by_topic.get(t, 0) + 1

    shown_by_topic: Dict[str, int] = {}
    for it in picked:
        t = it.get("topic") or "未知"
        shown_by_topic[t] = shown_by_topic.get(t, 0) + 1

    # 生成摘要
    if args.no_llm_summary:
        for it in picked:
            c = clean_text(it.get("content_text", ""))
            it["summary"] = (c[:SUMMARY_MAX_CHARS].rstrip("，。；;:：") + "。") if c else ""
    else:
        mapping = llm_summarize(picked)
        for it in picked:
            it["summary"] = mapping.get(it["article_id"], "") or ""

    # 生成 markdown（你要的简洁列表）
    md = build_markdown_simple(run_date, picked)

    # 写入 repoter/ 目录
    md_path = write_markdown_file(args.output_dir, run_date, md)

    # 落库
    upsert_digest_run(
        conn=conn,
        run_date=run_date,
        source_run_date=run_date,
        total_candidates=total_candidates,
        total_selected=len(picked),
        digest_text=md,
        channel=args.channel
    )
    insert_digest_items(conn, run_date, picked)
    insert_topic_stats(conn, run_date, total_by_topic, shown_by_topic)
    conn.commit()

    # 可选：标记已推送（写 digest_sent）
    if args.mark_sent:
        mark_sent(conn, run_date, [int(it["article_id"]) for it in picked])

    conn.close()

    print(f"✅ Digest generated run_date={run_date}")
    print(f"✅ total_candidates={total_candidates}, total_selected={len(picked)}")
    print(f"✅ Markdown written: {md_path}")
    if args.mark_sent:
        print("✅ Marked as sent (digest_sent updated).")


if __name__ == "__main__":
    main()
