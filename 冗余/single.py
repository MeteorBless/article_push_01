# -*- coding: utf-8 -*-
"""
pipeline_topic_tagging_single.py

功能：
- 从 articles_deduped 读取某天（默认昨天）的去重结果
- 核心逻辑支持开关配置（ENABLE_RULES / ENABLE_LLM）：
  1. 仅规则：命中即写入（强行自信），未命中则忽略。
  2. 仅LLM：全量调用LLM。
  3. 混合（默认）：规则高置信度优先写入 -> 规则低置信度/未命中 -> 转LLM。
- 写入 SQLite 表 article_topics（单标签：run_date+article_id 唯一）

依赖：
pip install requests
"""

import os
import re
import json
import time
import sqlite3
import datetime
from typing import Dict, Any, List, Tuple, Optional

import requests

# =========================
# 配置区
# =========================

# --- 功能开关 ---
ENABLE_RULES = False  # 是否启用规则匹配
ENABLE_LLM = True  # 是否启用大模型 (如果规则未命中或不确信，是否由LLM兜底)

# ----------------
DB_PATH = "news.db"

TODAY = datetime.date.today()
RUN_DATE = TODAY - datetime.timedelta(days=3)
# 如果需要指定日期跑，取消下面注释
# RUN_DATE = datetime.date(2026, 1, 31)

DEDUPED_TABLE = "articles_deduped"
TOPIC_TABLE = "article_topics"

# 6个主题（固定枚举）
TOPICS = ["党建时政", "非传统安全", "国际", "十五五", "AI学习", "法律条文"]

# 规则命中阈值：>=1 认为命中
RULE_HIT_THRESHOLD = 1

# 冲突判定：top1 与 top2 分差 <= 这个值 => 不确定，交给 LLM
CONFLICT_MAX_GAP = 0

# LLM（硅基流动）
LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"
SILICONFLOW_API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"

TEMPERATURE = 0.0
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 0.6

# 给 LLM 的正文预览
CONTENT_PREVIEW_LEN = 5000

# LLM 每批处理多少条
LLM_BATCH_SIZE = 20

# 日志 / raw 目录
LOG_DIR = "logs"
RAW_DIR = os.path.join(LOG_DIR, "llm_topic_raw_logs")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)


# =========================


def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def day_str(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def extract_first_json(text: str) -> str:
    text = (text or "").strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError(f"无法提取JSON，输出前200字：{text[:200]}")
    return m.group(0)


# =========================
# 建表（单标签版）
# =========================
def ensure_topic_table(conn: sqlite3.Connection):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TOPIC_TABLE} (
      run_date   TEXT NOT NULL,
      article_id INTEGER NOT NULL,
      topic      TEXT NOT NULL,      -- 6选1
      confidence REAL,
      method     TEXT NOT NULL,      -- rule / llm
      reason     TEXT,
      created_at TEXT NOT NULL,
      PRIMARY KEY (run_date, article_id)
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TOPIC_TABLE}_run_date ON {TOPIC_TABLE}(run_date);")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TOPIC_TABLE}_article_id ON {TOPIC_TABLE}(article_id);")
    conn.commit()


def delete_topics_for_day(conn: sqlite3.Connection, run_date: str):
    conn.execute(f"DELETE FROM {TOPIC_TABLE} WHERE run_date = ?", (run_date,))
    conn.commit()


# =========================
# 读取 deduped
# =========================
def fetch_deduped_for_run_date(conn: sqlite3.Connection, run_date: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT article_id,
               COALESCE(title,'') as title,
               COALESCE(url,'') as url,
               COALESCE(source,'') as source,
               COALESCE(pub_time,'') as pub_time,
               COALESCE(content_text,'') as content_text,
               COALESCE(dedup_group_id,'') as dedup_group_id,
               COALESCE(decision,'') as decision
        FROM {DEDUPED_TABLE}
        WHERE dedup_run_date = ?
        ORDER BY pub_time DESC, article_id DESC
        """,
        (run_date,)
    ).fetchall()

    items = []
    for r in rows:
        items.append({
            "article_id": int(r[0]),
            "title": clean_text(r[1]),
            "url": r[2],
            "source": r[3],
            "pub_time": r[4],
            "content_text": r[5] or "",
            "dedup_group_id": r[6],
            "decision": r[7],
        })
    return items


# =========================
# 规则关键词
# =========================
RULES: Dict[str, List[str]] = {
    "十五五": [
        "十五五", "第十五个五年规划", "十五五规划", "十五五时期"
    ],
    "法律条文": [
        "条例", "办法", "规定", "细则", "通知", "意见", "决定", "司法解释", "解释", "法",
        "修正案", "草案", "征求意见稿", "施行", "公布", "发布", "起草说明", "立法", "执法",
        "第[一二三四五六七八九十百千0-9]+条", "第[0-9]+条"
    ],
    "党建时政": [
        "党委", "党组", "党支部", "党建", "党纪", "巡视", "巡察", "纪检", "监察", "中央",
        "总书记", "政治局", "全会", "常委会", "主题教育", "党风廉政", "组织部", "宣传部",
        "干部", "任免", "会议精神", "学习贯彻"
    ],
    "AI学习": [
        "AI", "人工智能", "大模型", "LLM", "prompt", "提示词", "agent", "RAG", "微调", "蒸馏",
        "embedding", "向量", "推理", "对齐", "模型", "算力", "训练", "教程", "课程", "学习", "指南",
        "LangChain", "LlamaIndex", "Transformer"
    ],
    "国际": [
        "联合国", "欧盟", "北约", "G7", "G20", "峰会", "外长", "外交部", "使馆", "大使", "制裁",
        "俄乌", "中东", "巴以", "以色列", "伊朗", "美国", "日本", "韩国", "英国", "法国", "德国",
        "东盟", "APEC", "金砖", "WTO", "IMF", "世界银行"
    ],
    "非传统安全": [
        "网络安全", "数据安全", "信息安全", "舆情", "反恐", "反邪教", "生物安全", "公共卫生",
        "疫情", "传染病", "洪水", "台风", "地震", "极端天气", "灾害", "能源安全", "粮食安全",
        "供应链", "跨境犯罪", "电信诈骗", "诈骗", "洗钱", "毒品", "走私", "无人机", "关键基础设施"
    ],
}


def rule_score(text: str, patterns: List[str]) -> int:
    if not text:
        return 0
    s = 0
    for p in patterns:
        try:
            if re.search(p, text, flags=re.IGNORECASE):
                s += 1
        except re.error:
            if p.lower() in text.lower():
                s += 1
    return s


def classify_by_rules(item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    返回：
    - label: {topic, confidence, method, reason} 或 None (无命中)
    - needs_llm: bool（规则不确定/冲突 => True）
    """
    title = item.get("title", "") or ""
    content = item.get("content_text", "") or ""
    source = item.get("source", "") or ""
    merged_text = f"{title}\n{source}\n{content}"

    scores: Dict[str, int] = {}
    for t in TOPICS:
        scores[t] = rule_score(merged_text, RULES.get(t, []))

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    best_topic, best_sc = ranked[0]
    second_topic, second_sc = ranked[1] if len(ranked) > 1 else ("", 0)

    if best_sc < RULE_HIT_THRESHOLD:
        return None, True

    # 冲突检测
    needs_llm = False
    if second_sc >= RULE_HIT_THRESHOLD and (best_sc - second_sc) <= CONFLICT_MAX_GAP:
        needs_llm = True

    gap = best_sc - second_sc
    conf = 0.65 + min(0.30, 0.10 * max(0, gap))
    conf = round(float(conf), 3)

    label = {
        "topic": best_topic,
        "confidence": conf,
        "method": "rule",
        "reason": f"规则Top1={best_topic}({best_sc}) vs Top2={second_topic}({second_sc})"
    }
    return label, needs_llm


# =========================
# LLM 分类（单标签输出）
# =========================
def call_llm(messages: List[Dict[str, str]], raw_save_path: str) -> str:
    headers = {"Authorization": f"Bearer {SILICONFLOW_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": LLM_MODEL, "messages": messages, "temperature": TEMPERATURE}

    resp = requests.post(LLM_URL, headers=headers, json=payload, timeout=TIMEOUT_SECONDS)

    try:
        raw_obj = resp.json()
    except Exception:
        raw_obj = {"status_code": resp.status_code, "text": resp.text}

    with open(raw_save_path, "w", encoding="utf-8") as f:
        json.dump(raw_obj, f, ensure_ascii=False, indent=2)

    if resp.status_code != 200:
        raise RuntimeError(f"LLM 调用失败 HTTP {resp.status_code}：{resp.text[:2000]}")

    return raw_obj["choices"][0]["message"]["content"]


def build_topic_prompt_single(items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    system = "你是新闻主题分类器。你必须只输出严格JSON，不能输出任何额外文字或markdown。"

    payload = {
        "task": "classify_topic_single",
        "allowed_topics": TOPICS,
        "rules": [
            "每条新闻必须且仅分配 1 个主题。",
            "主题只能来自 allowed_topics。",
            "如果不确定，选择最贴近的一类。",
            "十五五/法律条文/党建时政属于强专题，命中时优先。"
        ],
        "output_format": {
            "items": [
                {
                    "article_id": "int",
                    "topic": "one of allowed_topics",
                    "confidence": "0~1 float",
                    "reason": "string<=40字"
                }
            ]
        },
        "items": []
    }

    for it in items:
        payload["items"].append({
            "article_id": it["article_id"],
            "title": it.get("title", ""),
            "source": it.get("source", ""),
            "pub_time": it.get("pub_time", ""),
            "url": it.get("url", ""),
            "content_preview": (it.get("content_text") or "")[:CONTENT_PREVIEW_LEN]
        })

    user = (
        "请对下列新闻逐条进行主题分类。必须只输出严格JSON，topic只能来自 allowed_topics。\n\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def normalize_llm_single(obj: Dict[str, Any], requested_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    allowed = set(TOPICS)
    req = set(int(x) for x in requested_ids)

    out: Dict[int, Dict[str, Any]] = {}
    items = obj.get("items") or []
    for it in items:
        try:
            aid = int(it.get("article_id"))
        except Exception:
            continue
        if aid not in req:
            continue

        topic = str(it.get("topic") or "").strip()
        if topic not in allowed:
            continue

        conf = it.get("confidence", None)
        try:
            conf = float(conf) if conf is not None else None
        except Exception:
            conf = None

        reason = str(it.get("reason") or "").strip()
        out[aid] = {"topic": topic, "confidence": conf, "reason": reason}

    return out


# =========================
# 写入 topic 表
# =========================
def upsert_topic_row(
        conn: sqlite3.Connection,
        run_date: str,
        article_id: int,
        topic: str,
        confidence: Optional[float],
        method: str,
        reason: str
):
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {TOPIC_TABLE}
          (run_date, article_id, topic, confidence, method, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_date, int(article_id), topic, confidence, method, reason, now_ts())
    )


# =========================
# 主流程
# =========================
def main():
    run_date = day_str(RUN_DATE)

    topic_log_path = os.path.join(LOG_DIR, f"topic_tagging_log_{run_date}.txt")

    conn = sqlite3.connect(DB_PATH)
    ensure_topic_table(conn)

    items = fetch_deduped_for_run_date(conn, run_date)
    if not items:
        conn.close()
        raise RuntimeError(f"{DEDUPED_TABLE} 中未找到 dedup_run_date={run_date} 的数据")

    # 清空当天结果
    delete_topics_for_day(conn, run_date)

    llm_candidates: List[Dict[str, Any]] = []

    # 统计计数器
    rule_written = 0
    rule_skipped_for_llm = 0
    rule_miss = 0

    with open(topic_log_path, "w", encoding="utf-8") as log_f:
        log_f.write("=== Topic Tagging (SWITCHABLE) ===\n")
        log_f.write(f"run_date={run_date}\n")
        log_f.write(f"ENABLE_RULES={ENABLE_RULES}\n")
        log_f.write(f"ENABLE_LLM={ENABLE_LLM}\n")
        log_f.write(f"total_items={len(items)}\n\n")

        # ---------------------------
        # A) 遍历判定
        # ---------------------------
        for it in items:
            processed_by_rule = False

            # 1. 如果开启了规则
            if ENABLE_RULES:
                label, is_weak_or_conflict = classify_by_rules(it)

                if label:
                    # 规则命中了
                    # 决策：是直接写入，还是转LLM？
                    if is_weak_or_conflict and ENABLE_LLM:
                        # 规则不够自信，且 LLM 开启 -> 交给 LLM (不写库)
                        rule_skipped_for_llm += 1
                        pass
                    else:
                        # 1. 规则很自信
                        # 2. 或者规则不够自信但 LLM 没开 (被迫自信，强行写入)
                        upsert_topic_row(
                            conn=conn,
                            run_date=run_date,
                            article_id=it["article_id"],
                            topic=label["topic"],
                            confidence=label.get("confidence"),
                            method="rule",
                            reason=label.get("reason", "")
                        )
                        rule_written += 1
                        processed_by_rule = True
                else:
                    # 规则未命中
                    rule_miss += 1

            # 2. 决定是否进入 LLM 队列
            # 如果文章已经被规则处理并写入了，就不进队列
            # 否则，如果 LLM 开关开启，则进队列
            if not processed_by_rule:
                if ENABLE_LLM:
                    llm_candidates.append(it)
                else:
                    # LLM 没开，且规则也没写入（可能是规则没开，或者规则未命中）
                    # 结果就是这条数据被丢弃（Unclassified）
                    pass

        conn.commit()

        log_f.write(f"Rules Processed:\n")
        log_f.write(f"  - Written directly: {rule_written}\n")
        log_f.write(f"  - Skipped (Weak/Conflict -> LLM): {rule_skipped_for_llm}\n")
        log_f.write(f"  - No Hit: {rule_miss}\n")
        log_f.write(f"LLM Candidates Queue Size: {len(llm_candidates)}\n\n")

        # ---------------------------
        # B) LLM 处理阶段
        # ---------------------------
        llm_written = 0
        if ENABLE_LLM and llm_candidates:
            for start in range(0, len(llm_candidates), LLM_BATCH_SIZE):
                batch = llm_candidates[start:start + LLM_BATCH_SIZE]
                batch_ids = [b["article_id"] for b in batch]

                messages = build_topic_prompt_single(batch)
                raw_path = os.path.join(RAW_DIR, f"llm_topic_raw_{run_date}_{start}_{int(time.time())}.json")

                log_f.write("=" * 60 + "\n")
                log_f.write(f"[{now_ts()}] LLM batch {start}-{start + len(batch) - 1}\n")

                try:
                    reply = call_llm(messages, raw_path)
                    parsed = json.loads(extract_first_json(reply))
                    mapping = normalize_llm_single(parsed, batch_ids)

                    for it2 in batch:
                        aid = it2["article_id"]
                        if aid not in mapping:
                            continue

                        m = mapping[aid]
                        upsert_topic_row(
                            conn=conn,
                            run_date=run_date,
                            article_id=aid,
                            topic=m["topic"],
                            confidence=m.get("confidence", None),
                            method="llm",
                            reason=m.get("reason", "")
                        )
                        llm_written += 1

                    log_f.write(f"[{now_ts()}] Batch done. Mapped: {len(mapping)}\n")

                except Exception as e:
                    log_f.write(f"[{now_ts()}] ❌ LLM Batch Error: {str(e)}\n")

                conn.commit()
                time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)
        else:
            log_f.write("LLM is DISABLED or Queue is empty.\n")

        # ---------------------------
        # 汇总
        # ---------------------------
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM {TOPIC_TABLE} WHERE run_date = ?",
            (run_date,)
        ).fetchone()[0]

        log_f.write("=" * 60 + "\n")
        log_f.write(f"[{now_ts()}] SUMMARY\n")
        log_f.write(f"Total Rows in DB: {cnt}\n")
        log_f.write(f"Method Breakdown: Rules={rule_written}, LLM={llm_written}\n")

    conn.close()
    print(f"✅ 完成。RunDate={run_date}, Rules={ENABLE_RULES}, LLM={ENABLE_LLM}")
    print(f"✅ 入库数: {rule_written + llm_written} (Rules: {rule_written}, LLM: {llm_written})")
    print(f"✅ 日志: {topic_log_path}")


if __name__ == "__main__":
    main()