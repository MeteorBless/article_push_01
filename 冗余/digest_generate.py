# -*- coding: utf-8 -*-
"""
digest_generate.py

生成“每日新闻速览”（日报）并写入 news.db 中的推送相关表：
- digest_runs
- digest_items
- digest_topic_stats
- digest_sent（可选：标记已推送）

依赖：仅标准库（sqlite3 / datetime / argparse 等）

输入来源：
- articles_deduped（去重后主条）
- article_topics（single 主题）

核心特性：
1) 动态 topic 展示：按当天各 topic 候选占比 + 最小/最大展示条数分配名额
2) 推送层防重：排除 digest_sent 中已推送过的 article_id
3) 结果可复现：digest_items 保存标题/来源/链接/摘要等“快照”
4) 支持重跑：默认会先删除同 run_date 的 digest_items / digest_topic_stats，再重写（--overwrite）

用法示例：
python digest_generate.py
python digest_generate.py --date 2026-02-01 --total-limit 50
python digest_generate.py --date 2026-02-01 --channel feishu --mark-sent --overwrite

注意：
- run_date 建议与 single.py 的 RUN_DATE 对齐（即 article_topics.run_date / articles_deduped.dedup_run_date）
"""

import argparse
import datetime
import re
import sqlite3
import os
import json
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import requests


# =========================
# 默认配置
# =========================
DB_PATH = "news.db"

DEFAULT_TOTAL_LIMIT = 50          # 每天最多推 50 条
DEFAULT_MIN_PER_TOPIC = 1         # 该 topic 有候选时，至少展示 1 条
DEFAULT_MAX_PER_TOPIC = 15        # 单个 topic 最多展示 15 条（防止一类“霸榜”）

SUMMARY_LEN = 90                  # 摘要默认截取长度（字符数）

# “权威来源”加分（可按你的业务继续扩充）
SOURCE_BOOST = {
    "人民网": 2.0,
    "新华社": 2.0,
    "光明网": 1.0,
    "央视": 1.5,
    "求是": 1.8,
}

# topic 展示顺序（可按你的推送习惯调整）
TOPIC_ORDER = ["党建时政", "十五五", "法律条文", "非传统安全", "国际", "AI学习"]



# =========================
# LLM 摘要配置（与 single.py 同口径）
# =========================
# LLM（硅基流动）
LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"
# 按你的要求：key 直接写在文件里（注意：不要把这个脚本随意上传到公网/仓库）
SILICONFLOW_API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"

TEMPERATURE = 0.2
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 0.5

CONTENT_PREVIEW_LEN = 900
LLM_BATCH_SIZE = 20

# =========================
# 数据结构
# =========================
@dataclass
class Candidate:
    article_id: int
    topic: str
    topic_confidence: Optional[float]
    topic_method: Optional[str]
    topic_reason: Optional[str]

    dedup_group_id: str
    decision: str

    pub_time: str
    source: str
    title: str
    url: str
    content_text: str

    score: float = 0.0
    summary: str = ""
    picked_reason: str = ""


# =========================
# 工具函数
# =========================
def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def day_from_str(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


def day_str(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def safe_summary(text: str, n: int = SUMMARY_LEN) -> str:
    t = clean_text(text)
    if not t:
        return ""
    return (t[:n] + "…") if len(t) > n else t


def extract_first_json(text: str) -> str:
    """从模型输出中提取最外层 JSON 对象（简单容错）。"""
    text = (text or "").strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError(f"无法提取JSON，输出前200字：{text[:200]}")
    return m.group(0)


def call_llm(messages: List[Dict[str, str]], timeout: int = TIMEOUT_SECONDS) -> str:
    headers = {"Authorization": f"Bearer {SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": LLM_MODEL, "messages": messages, "temperature": TEMPERATURE}
    resp = requests.post(LLM_URL, headers=headers, json=payload, timeout=timeout)

    if resp.status_code != 200:
        raise RuntimeError(f"LLM 调用失败 HTTP {resp.status_code}：{resp.text[:2000]}")
    obj = resp.json()
    return obj["choices"][0]["message"]["content"]


def build_one_line_summary_prompt(items: List[Candidate]) -> List[Dict[str, str]]:
    """批量生成“一句话新闻速览”摘要，严格 JSON 输出。"""
    system = (
        "你是新闻编辑。你必须只输出严格JSON，不能输出任何额外文字或markdown。\n"
        "请为每条新闻生成一句话‘日报速览’摘要：客观中性、信息密度高、不要口号、不要引用、不要换行。\n"
        "要求：中文为主，18~40字；尽量包含‘主体+动作+对象/影响’；不要出现‘本文’‘记者’等。\n"
        "输出必须匹配给定的 article_id。"

    )

    payload = {
        "task": "daily_digest_one_line_summaries",
        "constraints": {
            "length": "18~40 Chinese characters",
            "style": "objective, concise, no slogans",
            "no_newlines": True
        },
        "output_format": {
            "items": [{"article_id": "int", "summary": "string"}]
        },
        "items": []
    }

    for c in items:
        payload["items"].append({
            "article_id": c.article_id,
            "title": c.title,
            "source": c.source,
            "pub_time": c.pub_time,
            "url": c.url,
            "content_preview": (c.content_text or "")[:CONTENT_PREVIEW_LEN],
        })

    user = (
        "请按要求为每条新闻生成一句话摘要。必须只输出严格JSON。\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def llm_summarize_candidates(cands: List[Candidate], batch_size: int = LLM_BATCH_SIZE) -> Dict[int, str]:
    """返回 article_id -> summary 的映射。"""
    out: Dict[int, str] = {}
    if not cands:
        return out

    for start in range(0, len(cands), batch_size):
        batch = cands[start:start + batch_size]
        msgs = build_one_line_summary_prompt(batch)
        reply = call_llm(msgs)
        obj = json.loads(extract_first_json(reply))
        items = obj.get("items") or []
        for it in items:
            try:
                aid = int(it.get("article_id"))
            except Exception:
                continue
            s = clean_text(str(it.get("summary") or ""))
            if s:
                out[aid] = s
        time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)

    return out


def parse_time_guess(s: str) -> Optional[datetime.datetime]:
    """
    尝试解析 pub_time（常见格式：YYYY-MM-DD HH:MM:SS / YYYY-MM-DD HH:MM / YYYY-MM-DD）
    解析失败返回 None
    """
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s[:len(fmt)], fmt)
        except Exception:
            pass
    return None


def compute_score(c: Candidate, run_date: str) -> float:
    """
    一个轻量、可控的排序打分：
    - 时间越新越高
    - 权威来源加分
    - 正文长度适中加分
    - topic_confidence（如果有）微弱加分
    """
    score = 0.0

    # 1) recency（距 run_date 越近越好）
    rd = day_from_str(run_date)
    dt = parse_time_guess(c.pub_time)
    if dt:
        delta_days = abs((dt.date() - rd).days)
        score += max(0.0, 3.0 - 0.5 * delta_days)

    # 2) source boost
    for k, b in SOURCE_BOOST.items():
        if k in (c.source or ""):
            score += b
            break

    # 3) content length（太短像快讯、太长也不一定更好）
    L = len(clean_text(c.content_text))
    if L >= 800:
        score += 1.2
    elif L >= 300:
        score += 0.8
    elif L >= 120:
        score += 0.3
    else:
        score -= 0.2

    # 4) topic confidence 微弱加成
    if c.topic_confidence is not None:
        try:
            score += float(c.topic_confidence) * 0.6
        except Exception:
            pass

    return float(score)


# =========================
# 建表（推送层）
# =========================
def ensure_digest_tables(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS digest_runs (
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digest_runs_status ON digest_runs(status);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS digest_items (
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digest_items_run_topic ON digest_items(run_date, topic);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digest_items_article ON digest_items(article_id);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS digest_topic_stats (
      run_date   TEXT NOT NULL,
      topic      TEXT NOT NULL,
      total_cnt  INTEGER NOT NULL,
      shown_cnt  INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_date, topic)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digest_topic_stats_run ON digest_topic_stats(run_date);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS digest_sent (
      article_id INTEGER PRIMARY KEY,
      first_sent_date TEXT NOT NULL,
      last_sent_date  TEXT NOT NULL,
      times_sent      INTEGER NOT NULL DEFAULT 1,
      created_at      TEXT NOT NULL
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_digest_sent_last ON digest_sent(last_sent_date);")

    conn.commit()


def clear_digest_for_day(conn: sqlite3.Connection, run_date: str):
    conn.execute("DELETE FROM digest_items WHERE run_date = ?", (run_date,))
    conn.execute("DELETE FROM digest_topic_stats WHERE run_date = ?", (run_date,))
    conn.commit()


# =========================
# 取数：候选集合（去重后 + 有topic + 未推送过）
# =========================
def fetch_candidates(conn: sqlite3.Connection, run_date: str) -> List[Candidate]:
    rows = conn.execute(
        """
        SELECT
          t.article_id,
          t.topic,
          t.confidence,
          t.method,
          t.reason,

          COALESCE(d.dedup_group_id,'') as dedup_group_id,
          COALESCE(d.decision,'')       as decision,

          COALESCE(d.pub_time,'')       as pub_time,
          COALESCE(d.source,'')         as source,
          COALESCE(d.title,'')          as title,
          COALESCE(d.url,'')            as url,
          COALESCE(d.content_text,'')   as content_text
        FROM article_topics t
        JOIN articles_deduped d
          ON t.article_id = d.article_id
         AND t.run_date = d.dedup_run_date
        LEFT JOIN digest_sent s
          ON s.article_id = d.article_id
        WHERE t.run_date = ?
          AND s.article_id IS NULL
        ORDER BY t.topic, d.pub_time DESC, d.article_id DESC
        """,
        (run_date,)
    ).fetchall()

    out: List[Candidate] = []
    for r in rows:
        out.append(Candidate(
            article_id=int(r[0]),
            topic=r[1],
            topic_confidence=r[2],
            topic_method=r[3],
            topic_reason=r[4],
            dedup_group_id=r[5],
            decision=r[6],
            pub_time=r[7],
            source=r[8],
            title=r[9],
            url=r[10],
            content_text=r[11],
        ))
    return out


# =========================
# 动态展示名额分配
# =========================
def allocate_slots(
    topic_counts: Dict[str, int],
    total_limit: int,
    min_per_topic: int,
    max_per_topic: int,
) -> Dict[str, int]:
    topics = [t for t, c in topic_counts.items() if c > 0]
    if not topics:
        return {}

    alloc: Dict[str, int] = {}
    base_sum = 0
    for t in topics:
        base = min(topic_counts[t], min_per_topic)
        alloc[t] = base
        base_sum += base

    remaining = max(0, total_limit - base_sum)
    if remaining <= 0:
        return alloc

    total_cnt = sum(topic_counts[t] for t in topics)
    if total_cnt <= 0:
        return alloc

    remainders: List[Tuple[float, str]] = []
    for t in topics:
        cap = min(topic_counts[t], max_per_topic)
        room = max(0, cap - alloc[t])
        if room <= 0:
            continue

        exact = remaining * (topic_counts[t] / total_cnt)
        add_int = int(exact)
        add = min(room, add_int)
        alloc[t] += add
        remaining -= add
        remainders.append((exact - add_int, t))

    if remaining <= 0:
        return alloc

    remainders.sort(reverse=True, key=lambda x: x[0])
    idx = 0
    safe_guard = 10000
    while remaining > 0 and safe_guard > 0:
        safe_guard -= 1
        if not remainders:
            break
        _, t = remainders[idx % len(remainders)]
        cap = min(topic_counts[t], max_per_topic)
        if alloc[t] < cap:
            alloc[t] += 1
            remaining -= 1
        idx += 1

        if all(alloc[x] >= min(topic_counts[x], max_per_topic) for x in topics):
            break

    return alloc


def topic_sort_key(topic: str) -> Tuple[int, str]:
    try:
        return (TOPIC_ORDER.index(topic), topic)
    except ValueError:
        return (999, topic)


# =========================
# 生成 Markdown
# =========================
def render_markdown(run_date: str, topic_items: Dict[str, List[Candidate]], topic_shown: Dict[str, int]) -> str:
    total = sum(topic_shown.get(t, 0) for t in topic_items.keys())
    lines: List[str] = []
    lines.append(f"# 🗞️ 每日新闻速览（{run_date}）")
    lines.append("")
    lines.append(f"去重后入选：**{total}** 条（已按推送历史去重）")
    lines.append("")

    overview = []
    for t in sorted(topic_items.keys(), key=topic_sort_key):
        shown = int(topic_shown.get(t, 0))
        total_cnt = len(topic_items[t])
        if total_cnt <= 0 or shown <= 0:
            continue
        overview.append(f"{t} **{shown}/{total_cnt}**")
    if overview:
        lines.append("**主题概览：** " + " ｜ ".join(overview))
        lines.append("")

    for t in sorted(topic_items.keys(), key=topic_sort_key):
        items = topic_items[t]
        shown = int(topic_shown.get(t, 0))
        if shown <= 0:
            continue
        lines.append(f"## {t}（{shown}条）")
        for c in items[:shown]:
            src = c.source or "来源未知"
            pt = c.pub_time or ""
            title = c.title or "(无标题)"
            url = c.url or ""
            summary = c.summary or ""
            if url:
                lines.append(f"- [{title}]({url})（{src}｜{pt}）")
            else:
                lines.append(f"- {title}（{src}｜{pt}）")
            if summary:
                lines.append(f"  - {summary}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


# =========================
# 写入 digest_* 表
# =========================
def write_digest(
    conn: sqlite3.Connection,
    run_date: str,
    channel: Optional[str],
    title: str,
    markdown_text: str,
    selected: List[Candidate],
    topic_counts: Dict[str, int],
    topic_shown: Dict[str, int],
):
    created = now_ts()

    conn.execute(
        """
        INSERT OR REPLACE INTO digest_runs
          (run_date, created_at, source_run_date, total_candidates, total_selected, status, channel, digest_title, digest_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_date, created, run_date, int(sum(topic_counts.values())), int(len(selected)), "draft", channel, title, markdown_text)
    )

    for t, cnt in topic_counts.items():
        shown = int(topic_shown.get(t, 0))
        conn.execute(
            """
            INSERT OR REPLACE INTO digest_topic_stats (run_date, topic, total_cnt, shown_cnt, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_date, t, int(cnt), int(shown), created)
        )

    for idx, c in enumerate(selected, start=1):
        conn.execute(
            """
            INSERT OR REPLACE INTO digest_items (
              run_date, item_no,
              topic, topic_confidence, topic_method, topic_reason,
              article_id, dedup_group_id, decision,
              pub_time, source, title, url,
              summary, score, picked_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_date, idx,
                c.topic, c.topic_confidence, c.topic_method, c.topic_reason,
                c.article_id, c.dedup_group_id, c.decision,
                c.pub_time, c.source, c.title, c.url,
                c.summary, c.score, c.picked_reason, created
            )
        )

    conn.commit()


def mark_sent(conn: sqlite3.Connection, run_date: str, channel: Optional[str] = None):
    ts = now_ts()

    conn.execute(
        """
        UPDATE digest_runs
        SET status='sent', send_at=?, channel=COALESCE(channel, ?)
        WHERE run_date=?
        """,
        (ts, channel, run_date)
    )

    ids = conn.execute("SELECT article_id FROM digest_items WHERE run_date = ? ORDER BY item_no", (run_date,)).fetchall()
    for (aid,) in ids:
        row = conn.execute("SELECT first_sent_date, times_sent FROM digest_sent WHERE article_id=?", (int(aid),)).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO digest_sent(article_id, first_sent_date, last_sent_date, times_sent, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(aid), run_date, run_date, 1, ts)
            )
        else:
            first_sent_date, times_sent = row
            conn.execute(
                """
                UPDATE digest_sent
                SET last_sent_date=?, times_sent=?, created_at=?
                WHERE article_id=?
                """,
                (run_date, int(times_sent) + 1, ts, int(aid))
            )

    conn.commit()


# =========================
# 主流程
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB_PATH, help="SQLite DB path (default: news.db)")
    ap.add_argument("--date", default=None, help="run_date in YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--total-limit", type=int, default=DEFAULT_TOTAL_LIMIT, help="max items in digest (default: 50)")
    ap.add_argument("--min-per-topic", type=int, default=DEFAULT_MIN_PER_TOPIC, help="min shown per topic if exists")
    ap.add_argument("--max-per-topic", type=int, default=DEFAULT_MAX_PER_TOPIC, help="max shown per topic")
    ap.add_argument("--no-llm-summary", action="store_true", help="disable LLM one-line summaries; fallback to truncation")
    ap.add_argument("--llm-batch-size", type=int, default=LLM_BATCH_SIZE, help="LLM batch size for summaries")
    ap.add_argument("--channel", default=None, help="feishu/wecom/email... (optional)")
    ap.add_argument("--mark-sent", action="store_true", help="mark digest as sent and update digest_sent")
    ap.add_argument("--overwrite", action="store_true", help="overwrite existing digest_items/stats for that day")
    ap.add_argument("--output-dir", default="repoter", help="directory to write markdown file (default: repoter)")
    args = ap.parse_args()

    if args.date:
        run_date = args.date.strip()
        _ = day_from_str(run_date)  # validate
    else:
        today = datetime.date.today()
        run_date = day_str(today - datetime.timedelta(days=3))

    conn = sqlite3.connect(args.db)
    try:
        ensure_digest_tables(conn)

        if args.overwrite:
            clear_digest_for_day(conn, run_date)

        candidates = fetch_candidates(conn, run_date)
        if not candidates:
            raise RuntimeError(
                f"No candidates found for run_date={run_date}. "
                f"Check article_topics.run_date and articles_deduped.dedup_run_date, and digest_sent filter."
            )

        for c in candidates:
            # 先计算排序分（摘要后面用 LLM 生成）
            c.score = compute_score(c, run_date)

        by_topic: Dict[str, List[Candidate]] = {}
        for c in candidates:
            by_topic.setdefault(c.topic, []).append(c)
        for t in by_topic.keys():
            by_topic[t].sort(key=lambda x: (x.score, x.pub_time, x.article_id), reverse=True)

        topic_counts = {t: len(lst) for t, lst in by_topic.items()}

        topic_shown = allocate_slots(
            topic_counts=topic_counts,
            total_limit=int(args.total_limit),
            min_per_topic=int(args.min_per_topic),
            max_per_topic=int(args.max_per_topic),
        )

        selected: List[Candidate] = []
        for t in sorted(by_topic.keys(), key=topic_sort_key):
            shown = int(topic_shown.get(t, 0))
            if shown <= 0:
                continue
            for c in by_topic[t][:shown]:
                c.picked_reason = f"score={c.score:.2f}"
                selected.append(c)

        selected = selected[: int(args.total_limit)]


        # 生成“一句话速览”摘要：优先 LLM，失败则回退为截断正文
        if args.no_llm_summary:
            for c in selected:
                c.summary = safe_summary(c.content_text, SUMMARY_LEN)
        else:
            try:
                mapping = llm_summarize_candidates(selected, batch_size=int(args.llm_batch_size))
            except Exception as e:
                mapping = {}
                print(f"⚠️ LLM 摘要生成失败，将回退为截断正文。错误：{e}")

            for c in selected:
                if mapping.get(c.article_id):
                    c.summary = mapping[c.article_id]
                else:
                    c.summary = safe_summary(c.content_text, SUMMARY_LEN)

        title = f"每日新闻速览（{run_date}）"
        markdown_text = render_markdown(run_date, by_topic, topic_shown)

        write_digest(
            conn=conn,
            run_date=run_date,
            channel=args.channel,
            title=title,
            markdown_text=markdown_text,
            selected=selected,
            topic_counts=topic_counts,
            topic_shown=topic_shown,
        )

        # 写入 Markdown 文件到 output-dir（默认 repoter/）
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        md_path = out_dir / f"daily_digest_{run_date}.md"
        md_path.write_text(markdown_text, encoding="utf-8")
        print(f"✅ Markdown written: {md_path}")


        if args.mark_sent:
            mark_sent(conn, run_date, channel=args.channel)

        print(f"✅ Digest generated for run_date={run_date}")
        print(f"✅ candidates={len(candidates)} selected={len(selected)} total_limit={args.total_limit}")
        print("✅ written tables: digest_runs / digest_items / digest_topic_stats")
        if args.mark_sent:
            print("✅ marked as sent and updated digest_sent")

        print("\n" + markdown_text)

    finally:
        conn.close()


if __name__ == "__main__":
    main()