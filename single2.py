# -*- coding: utf-8 -*-
"""
pipeline_topic_tagging_single.py

新增功能：
- 可配置 ENABLE_RULE / ENABLE_LLM 开关：可都开、只开其一
- 当 LLM 关闭时，对规则冲突/未命中的兜底行为可配置
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
DB_PATH = "news.db"

TODAY = datetime.date.today()
RUN_DATE = TODAY - datetime.timedelta(days=1)
# RUN_DATE = datetime.date(2026, 1, 31)

DEDUPED_TABLE = "articles_deduped"
TOPIC_TABLE = "article_topics"

TOPICS = ["党建时政", "非传统安全", "国际", "十五五", "AI学习", "法律条文"]

RULE_HIT_THRESHOLD = 1
CONFLICT_MAX_GAP = 0

# ======= 新增：开关 =======
ENABLE_RULE = False
ENABLE_LLM = True

# 当 ENABLE_LLM=False 时，规则冲突/未命中的处理：
# True：强制写入规则Top1（即使低于阈值/冲突），保证每条都有topic
# False：不写入，记录到日志（更保守）
FORCE_RULE_FALLBACK_WHEN_NO_LLM = True
# =========================

LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"

# 建议改成环境变量（避免明文key泄露）
SILICONFLOW_API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"
# 如果你坚持写死，也可以把上面那行改回你的 key，但不建议。

TEMPERATURE = 0.0
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 0.6

CONTENT_PREVIEW_LEN = 900
LLM_BATCH_SIZE = 20

LOG_DIR = "logs"
RAW_DIR = os.path.join(LOG_DIR, "llm_topic_raw_logs")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)


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
      topic      TEXT NOT NULL,
      confidence REAL,
      method     TEXT NOT NULL,
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
    "十五五": ["十五五", "第十五个五年规划", "十五五规划", "十五五时期"],
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


def compute_rule_ranking(item: Dict[str, Any]) -> List[Tuple[str, int]]:
    title = item.get("title", "") or ""
    content = item.get("content_text", "") or ""
    source = item.get("source", "") or ""
    merged_text = f"{title}\n{source}\n{content}"

    scores: Dict[str, int] = {}
    for t in TOPICS:
        scores[t] = rule_score(merged_text, RULES.get(t, []))

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return ranked


def classify_by_rules(item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool, List[Tuple[str, int]]]:
    """
    返回：
    - label: {topic, confidence, method, reason} 或 None
    - needs_llm: bool（规则不确定/冲突 => True）
    - ranked: [(topic, score)...] 用于 fallback
    """
    ranked = compute_rule_ranking(item)
    best_topic, best_sc = ranked[0]
    second_topic, second_sc = ranked[1] if len(ranked) > 1 else ("", 0)

    if best_sc < RULE_HIT_THRESHOLD:
        return None, True, ranked

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
    return label, needs_llm, ranked


# =========================
# LLM 分类（单标签输出）
# =========================
def call_llm(messages: List[Dict[str, str]], raw_save_path: str) -> str:
    if not SILICONFLOW_API_KEY:
        raise RuntimeError("SILICONFLOW_API_KEY 未设置（ENABLE_LLM=True 时必须提供）")

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
# 写入 topic 表（单标签）
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
    if not ENABLE_RULE and not ENABLE_LLM:
        raise RuntimeError("ENABLE_RULE 和 ENABLE_LLM 不能同时为 False（否则无法分类）")

    run_date = day_str(RUN_DATE)
    topic_log_path = os.path.join(LOG_DIR, f"topic_tagging_log_{run_date}.txt")

    conn = sqlite3.connect(DB_PATH)
    ensure_topic_table(conn)

    items = fetch_deduped_for_run_date(conn, run_date)
    if not items:
        conn.close()
        raise RuntimeError(f"{DEDUPED_TABLE} 中未找到 dedup_run_date={run_date} 的数据")

    # 重跑：清空当天结果
    delete_topics_for_day(conn, run_date)

    llm_candidates: List[Dict[str, Any]] = []

    rule_written = 0
    rule_conflict = 0
    rule_nohit = 0
    rule_forced = 0
    skipped_no_llm = 0

    with open(topic_log_path, "w", encoding="utf-8") as log_f:
        log_f.write("=== Topic Tagging (SINGLE LABEL - WITH SWITCHES) ===\n")
        log_f.write(f"run_date={run_date}\n")
        log_f.write(f"ENABLE_RULE={ENABLE_RULE}\n")
        log_f.write(f"ENABLE_LLM={ENABLE_LLM}\n")
        log_f.write(f"FORCE_RULE_FALLBACK_WHEN_NO_LLM={FORCE_RULE_FALLBACK_WHEN_NO_LLM}\n")
        log_f.write(f"rules_threshold={RULE_HIT_THRESHOLD}\n")
        log_f.write(f"conflict_gap={CONFLICT_MAX_GAP}\n")
        log_f.write(f"llm_model={LLM_MODEL}\n")
        log_f.write(f"total_items={len(items)}\n")
        log_f.write(f"generated_at={now_ts()}\n\n")

        # A) 规则阶段（可开关）
        for it in items:
            if not ENABLE_RULE:
                # 不走规则：全部送LLM（如果LLM开）
                if ENABLE_LLM:
                    llm_candidates.append(it)
                else:
                    # 理论不会到这里（上面已禁止全关）
                    pass
                continue

            label, needs_llm, ranked = classify_by_rules(it)

            if label is None:
                rule_nohit += 1
                if ENABLE_LLM:
                    llm_candidates.append(it)
                else:
                    # LLM 关：看是否强制写规则Top1
                    if FORCE_RULE_FALLBACK_WHEN_NO_LLM:
                        best_topic, best_sc = ranked[0]
                        upsert_topic_row(
                            conn=conn,
                            run_date=run_date,
                            article_id=it["article_id"],
                            topic=best_topic,
                            confidence=0.50,  # 低置信度
                            method="rule_forced",
                            reason=f"LLM关闭且规则未命中，强制Top1={best_topic}({best_sc})"
                        )
                        rule_forced += 1
                    else:
                        skipped_no_llm += 1
                continue

            if needs_llm:
                rule_conflict += 1
                if ENABLE_LLM:
                    llm_candidates.append(it)
                else:
                    if FORCE_RULE_FALLBACK_WHEN_NO_LLM:
                        # 冲突也强制写Top1，但置信度更低
                        upsert_topic_row(
                            conn=conn,
                            run_date=run_date,
                            article_id=it["article_id"],
                            topic=label["topic"],
                            confidence=0.55,
                            method="rule_forced",
                            reason=f"LLM关闭且规则冲突，强制写Top1。{label.get('reason','')}"
                        )
                        rule_forced += 1
                    else:
                        skipped_no_llm += 1
                continue

            # 规则命中且确信 -> 写入
            upsert_topic_row(
                conn=conn,
                run_date=run_date,
                article_id=it["article_id"],
                topic=label["topic"],
                confidence=label.get("confidence", None),
                method="rule",
                reason=label.get("reason", "")
            )
            rule_written += 1

        conn.commit()

        log_f.write(f"rule_written_directly={rule_written}\n")
        log_f.write(f"rule_conflict_pushed_to_llm={rule_conflict}\n")
        log_f.write(f"rule_nohit_pushed_to_llm={rule_nohit}\n")
        log_f.write(f"rule_forced_written={rule_forced}\n")
        log_f.write(f"skipped_when_no_llm={skipped_no_llm}\n")
        log_f.write(f"total_llm_candidates={len(llm_candidates)}\n\n")

        # B) LLM 阶段（可开关）
        llm_written = 0
        if ENABLE_LLM and llm_candidates:
            for start in range(0, len(llm_candidates), LLM_BATCH_SIZE):
                batch = llm_candidates[start:start + LLM_BATCH_SIZE]
                batch_ids = [b["article_id"] for b in batch]

                messages = build_topic_prompt_single(batch)
                raw_path = os.path.join(RAW_DIR, f"llm_topic_raw_{run_date}_{start}_{int(time.time())}.json")

                log_f.write("=" * 92 + "\n")
                log_f.write(f"[{now_ts()}] LLM batch {start}-{start + len(batch) - 1} size={len(batch)} raw={raw_path}\n")
                log_f.write(f"[{now_ts()}] ▶ REQUEST(user snippet)\n{messages[1]['content'][:500]}...\n\n")

                try:
                    reply = call_llm(messages, raw_path)
                    log_f.write(f"[{now_ts()}] ◀ RESPONSE(raw len={len(reply)})\n")

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

                    log_f.write(f"[{now_ts()}] ✅ LLM mapped={len(mapping)} llm_written={llm_written}\n\n")

                except Exception as e:
                    log_f.write(f"[{now_ts()}] ❌ LLM Batch Error: {str(e)}\n\n")

                conn.commit()
                time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)

        # 汇总
        cnt = conn.execute(
            f"SELECT COUNT(*) FROM {TOPIC_TABLE} WHERE run_date = ?",
            (run_date,)
        ).fetchone()[0]

        log_f.write("=" * 92 + "\n")
        log_f.write(f"[{now_ts()}] SUMMARY\n")
        log_f.write(f"total_topic_rows_in_db={cnt}\n")
        log_f.write(f"breakdown: rule_confirmed={rule_written} + rule_forced={rule_forced} + llm_written={llm_written}\n")

    conn.close()

    print(f"✅ 主题打标完成 run_date={run_date}")
    print(f"✅ 开关：ENABLE_RULE={ENABLE_RULE}, ENABLE_LLM={ENABLE_LLM}")
    print(f"✅ 写入表：{TOPIC_TABLE}")
    print(f"✅ 日志：{topic_log_path}")
    print(f"✅ LLM raw：{RAW_DIR}/")


if __name__ == "__main__":
    main()
