# -*- coding: utf-8 -*-
"""
pipeline_daily_dedup.py

升级目标：
- 逻辑层与IO层解耦
- candidate_groups 不再必须落 JSON
- 可选输出：JSON / DB / BOTH
- 可选输入：MEMORY / DB / JSON
- 最终写入 articles_deduped（含孤立文章）

依赖：
pip install numpy requests
"""

import os
import re
import json
import time
import sqlite3
import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

import numpy as np
import requests

# ================== 全局配置 ==================
DB_PATH = "news.db"

TODAY = datetime.date.today()
RUN_DATE = TODAY - datetime.timedelta(days=1)  # 每天早上跑昨天
# RUN_DATE = datetime.date(2026, 1, 29)

EMBED_MODEL = "Qwen/Qwen3-Embedding-4B"
SIM_THRESHOLD = 0.85

# Candidate 输出开关：你要的“可选生成json、写DB表、俩个都选”
CAND_OUTPUT = {
    "json": True,
    "db": True
}

# Dedup 输入选择：优先 MEMORY（同进程），也可改 DB/JSON 复盘
DEDUP_INPUT_MODE = "MEMORY"  # "MEMORY" | "DB" | "JSON"

# DB：candidate_groups 表（用于存候选关系，替代 JSON）
CAND_TABLE = "candidate_groups"

# LLM（硅基流动）
LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"
#API_KEY = os.getenv("SILICONFLOW_API_KEY", "YOUR_API_KEY_HERE")
API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"
TEMPERATURE = 0.2
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 0.8
CONTENT_PREVIEW_LEN = 1800

# 最终表（你已建好）
DEDUPED_TABLE = "articles_deduped"
DELETE_EXISTING_RUN_DATE = True

# 中间与日志文件（可选）
OUT_DIR = ".."
LOG_DIR = "../logs"
RAW_DIR = "llm_raw_logs"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# 是否导出最终 JSON
EXPORT_FINAL_JSON = True
# =============================================


def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def day_str(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


# -------------------- DB: 基础读取 --------------------
def fetch_articles_for_day(conn: sqlite3.Connection, d: datetime.date) -> List[Dict[str, Any]]:
    ds = day_str(d)

    rows = conn.execute(
        """
        SELECT id, COALESCE(title,''), COALESCE(url,''), COALESCE(source,''), COALESCE(pub_time,''),
               COALESCE(content_text,'')
        FROM articles
        WHERE pub_time LIKE ?
        ORDER BY pub_time DESC, id DESC
        """,
        (f"{ds}%",)
    ).fetchall()

    if not rows:
        rows = conn.execute(
            """
            SELECT id, COALESCE(title,''), COALESCE(url,''), COALESCE(source,''), COALESCE(pub_time,''),
                   COALESCE(content_text,'')
            FROM articles
            WHERE fetched_at >= ? AND fetched_at <= ?
            ORDER BY fetched_at DESC, id DESC
            """,
            (f"{ds} 00:00:00", f"{ds} 23:59:59")
        ).fetchall()

    items = []
    for r in rows:
        items.append({
            "id": int(r[0]),
            "title": clean_text(r[1]),
            "url": r[2],
            "source": r[3],
            "pub_time": r[4],
            "content_text": r[5] or "",
        })
    return items


def fetch_embeddings(conn: sqlite3.Connection, article_ids: List[int]) -> Dict[int, np.ndarray]:
    if not article_ids:
        return {}

    q_marks = ",".join(["?"] * len(article_ids))
    rows = conn.execute(
        f"""
        SELECT article_id, embedding
        FROM article_embeddings
        WHERE model = ? AND article_id IN ({q_marks})
        """,
        [EMBED_MODEL, *article_ids]
    ).fetchall()

    out: Dict[int, np.ndarray] = {}
    for aid, emb_json in rows:
        try:
            out[int(aid)] = np.array(json.loads(emb_json), dtype=np.float32)
        except Exception:
            continue
    return out


# -------------------- 纯逻辑：构建候选组（不做IO） --------------------
def cosine_matrix(vectors: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1e-12, norms)
    v = vectors / norms
    return np.matmul(v, v.T)


def connected_components(n: int, edges: List[Tuple[int, int]]) -> List[List[int]]:
    parent = list(range(n))
    rank = [0] * n

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            parent[ra] = rb
        elif rank[ra] > rank[rb]:
            parent[rb] = ra
        else:
            parent[rb] = ra
            rank[ra] += 1

    for a, b in edges:
        union(a, b)

    comps: Dict[int, List[int]] = {}
    for i in range(n):
        r = find(i)
        comps.setdefault(r, []).append(i)

    return list(comps.values())  # 含 size=1


def build_candidate_groups(
    articles: List[Dict[str, Any]],
    emb_map: Dict[int, np.ndarray],
    threshold: float
) -> Dict[str, Any]:
    """
    返回候选结果（纯逻辑，无IO）：
    - groups: size>=2 的连通分量
    - isolated_items: size==1 的单点分量（无相似边）
    - missing_embedding_articles: 没 embedding 的文章
    - pairs_top: 方便日志/调参（可选）
    """
    kept = [a for a in articles if a["id"] in emb_map]
    missing = [a for a in articles if a["id"] not in emb_map]

    if len(kept) < 2:
        # 全部都当孤立（避免丢）
        return {
            "groups": [],
            "isolated_items": kept,
            "missing_embedding_articles": missing,
            "pairs_top": [],
        }

    vectors = np.stack([emb_map[a["id"]] for a in kept], axis=0)
    sim = cosine_matrix(vectors)

    edges: List[Tuple[int, int]] = []
    pairs: List[Tuple[int, int, float]] = []

    n = len(kept)
    for i in range(n):
        for j in range(i + 1, n):
            s = float(sim[i, j])
            if s >= threshold:
                edges.append((i, j))
                pairs.append((i, j, s))

    comps = connected_components(n, edges)
    groups_idx = [c for c in comps if len(c) >= 2]
    isolated_idx = [c[0] for c in comps if len(c) == 1]

    groups = []
    for gi, comp in enumerate(groups_idx, start=1):
        members = [kept[idx] for idx in comp]
        members_sorted = sorted(members, key=lambda x: x.get("pub_time") or "")
        groups.append({
            "group_id": f"g{gi}",
            "member_ids": [m["id"] for m in members_sorted],
            "members": members_sorted,
        })

    isolated_items = [kept[idx] for idx in isolated_idx]

    # pairs_top 取最高的前 N 条，便于你调阈值
    pairs_top = sorted(pairs, key=lambda x: x[2], reverse=True)[:200]

    return {
        "groups": groups,
        "isolated_items": isolated_items,
        "missing_embedding_articles": missing,
        "pairs_top": [
            {
                "a_id": kept[i]["id"],
                "b_id": kept[j]["id"],
                "sim": s,
                "a_title": kept[i]["title"][:120],
                "b_title": kept[j]["title"][:120],
            } for i, j, s in pairs_top
        ]
    }


# -------------------- IO：候选组输出（JSON / DB） --------------------
def dump_candidate_json(result: Dict[str, Any], run_date: str) -> str:
    path = os.path.join(OUT_DIR, f"candidate_groups_{run_date}.json")
    out = {
        "date": run_date,
        "model": EMBED_MODEL,
        "threshold": SIM_THRESHOLD,
        **result
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    return path


def ensure_candidate_table(conn: sqlite3.Connection):
    # 一张表把“组/孤立/缺embedding”都存下来
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {CAND_TABLE} (
        run_date TEXT NOT NULL,
        kind TEXT NOT NULL,          -- 'group' | 'isolated' | 'missing'
        group_id TEXT NOT NULL,      -- g1/g2... 或 iso/miss
        article_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (run_date, kind, group_id, article_id)
    );
    """)
    conn.commit()


def write_candidate_db(conn: sqlite3.Connection, result: Dict[str, Any], run_date: str, clear: bool = True):
    ensure_candidate_table(conn)
    if clear:
        conn.execute(f"DELETE FROM {CAND_TABLE} WHERE run_date = ?", (run_date,))
        conn.commit()

    ts = now_ts()

    # groups
    for g in result["groups"]:
        gid = g["group_id"]
        for aid in g["member_ids"]:
            conn.execute(
                f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
                (run_date, "group", gid, int(aid), ts)
            )

    # isolated
    for a in result["isolated_items"]:
        conn.execute(
            f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
            (run_date, "isolated", "iso", int(a["id"]), ts)
        )

    # missing embedding
    for a in result["missing_embedding_articles"]:
        conn.execute(
            f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
            (run_date, "missing", "miss", int(a["id"]), ts)
        )

    conn.commit()


def load_candidate_from_db(conn: sqlite3.Connection, run_date: str) -> Dict[str, Any]:
    ensure_candidate_table(conn)
    rows = conn.execute(
        f"SELECT kind, group_id, article_id FROM {CAND_TABLE} WHERE run_date = ?",
        (run_date,)
    ).fetchall()

    # 只从 DB 取 id，再回 articles 拉详情
    group_map: Dict[str, List[int]] = {}
    isolated_ids: List[int] = []
    missing_ids: List[int] = []

    for kind, gid, aid in rows:
        if kind == "group":
            group_map.setdefault(gid, []).append(int(aid))
        elif kind == "isolated":
            isolated_ids.append(int(aid))
        elif kind == "missing":
            missing_ids.append(int(aid))

    all_ids = sorted(set([*isolated_ids, *missing_ids, *[x for ids in group_map.values() for x in ids]]))

    # 拉文章详情
    articles_map = fetch_articles_by_ids(conn, all_ids)

    groups = []
    for gid, ids in sorted(group_map.items(), key=lambda x: x[0]):
        members = [articles_map[i] for i in ids if i in articles_map]
        members_sorted = sorted(members, key=lambda x: x.get("pub_time") or "")
        groups.append({"group_id": gid, "member_ids": [m["id"] for m in members_sorted], "members": members_sorted})

    isolated_items = [articles_map[i] for i in isolated_ids if i in articles_map]
    missing_items = [articles_map[i] for i in missing_ids if i in articles_map]

    return {
        "groups": groups,
        "isolated_items": isolated_items,
        "missing_embedding_articles": missing_items,
        "pairs_top": []
    }


def load_candidate_from_json(run_date: str) -> Dict[str, Any]:
    path = os.path.join(OUT_DIR, f"candidate_groups_{run_date}.json")
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)

    return {
        "groups": obj.get("groups", []) or [],
        "isolated_items": obj.get("isolated_items", []) or [],
        "missing_embedding_articles": obj.get("missing_embedding_articles", []) or [],
        "pairs_top": obj.get("pairs_top", []) or [],
    }


# -------------------- DB：辅助拉文章（给 dedup 用） --------------------
def fetch_articles_by_ids(conn: sqlite3.Connection, ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    q_marks = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""
        SELECT id, COALESCE(title,''), COALESCE(url,''), COALESCE(source,''), COALESCE(pub_time,''),
               COALESCE(content_text,'')
        FROM articles
        WHERE id IN ({q_marks})
        """,
        ids
    ).fetchall()

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        aid = int(r[0])
        out[aid] = {
            "id": aid,
            "title": clean_text(r[1]),
            "url": r[2],
            "source": r[3],
            "pub_time": r[4],
            "content_text": r[5] or "",
        }
    return out


# -------------------- Dedup：写入最终表 --------------------
def ensure_deduped_table_exists(conn: sqlite3.Connection):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (DEDUPED_TABLE,)
    ).fetchone()
    if not row:
        raise RuntimeError(f"数据库中不存在表 {DEDUPED_TABLE}（你说已建好，请检查库文件是否一致）")


def clear_deduped_run(conn: sqlite3.Connection, run_date: str):
    conn.execute(f"DELETE FROM {DEDUPED_TABLE} WHERE dedup_run_date = ?", (run_date,))
    conn.commit()


def insert_deduped(
    conn: sqlite3.Connection,
    run_date: str,
    group_id: str,
    article: Dict[str, Any],
    decision: str,
    confidence: Optional[float],
    reason: str
):
    conn.execute(
        f"""
        INSERT INTO {DEDUPED_TABLE} (
            dedup_run_date, dedup_group_id, article_id, title, url, source, pub_time, content_text,
            decision, confidence, reason, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_date,
            group_id,
            int(article["id"]),
            article.get("title", ""),
            article.get("url", ""),
            article.get("source", ""),
            article.get("pub_time", ""),
            article.get("content_text", ""),
            decision,
            confidence,
            reason,
            now_ts()
        )
    )


# -------------------- LLM 判重（仅对 groups） --------------------
def extract_first_json(text: str) -> str:
    text = (text or "").strip()
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError(f"无法提取JSON，输出前200字：{text[:200]}")
    return m.group(0)


def call_llm(messages: List[Dict[str, str]], raw_save_path: str) -> str:
    if not API_KEY or API_KEY == "YOUR_API_KEY_HERE":
        raise RuntimeError("请设置 SILICONFLOW_API_KEY（或把 API_KEY 写死到代码里）")

    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    payload = {"model": LLM_MODEL, "messages": messages, "temperature": TEMPERATURE}

    resp = requests.post(LLM_URL, headers=headers, json=payload, timeout=TIMEOUT_SECONDS)
    try:
        raw = resp.json()
    except Exception:
        raw = {"status_code": resp.status_code, "text": resp.text}

    with open(raw_save_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    if resp.status_code != 200:
        raise RuntimeError(f"LLM失败 HTTP {resp.status_code}: {resp.text[:2000]}")

    return raw["choices"][0]["message"]["content"]


def build_group_prompt(group_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    system = "你是新闻语义去重裁判。只输出严格JSON，不要解释，不要markdown。"
    payload = {
        "task": "dedup_and_pick_canonical",
        "output_format": {
            "decision": "merge or no_merge",
            "keep_id": "string(id from items)",
            "drop_ids": ["string(id from items)"],
            "reason": "string<=40字",
            "confidence": "0~1 float"
        },
        "items": []
    }
    for it in group_items:
        payload["items"].append({
            "id": str(it["id"]),
            "title": it.get("title", ""),
            "source": it.get("source", ""),
            "pub_time": it.get("pub_time", ""),
            "url": it.get("url", ""),
            "content_preview": (it.get("content_text") or "")[:CONTENT_PREVIEW_LEN]
        })

    user = (
        "判断该候选组是否应合并重复：\n"
        "- 合并：decision='merge'，keep_id主条，drop_ids重复条\n"
        "- 不合并：decision='no_merge'，drop_ids为空，keep_id任选其一\n"
        "必须只输出JSON，id必须来自items。\n\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def normalize_decision(parsed: Dict[str, Any], group_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    valid = {str(it["id"]) for it in group_items}
    keep_id = str(parsed.get("keep_id", "")).strip()
    drop_ids = [str(x).strip() for x in (parsed.get("drop_ids") or [])]
    decision = (parsed.get("decision") or "").strip()
    reason = (parsed.get("reason") or "").strip()
    confidence = parsed.get("confidence", None)

    if keep_id not in valid:
        keep_id = str(group_items[0]["id"])

    drop_ids = [x for x in drop_ids if x in valid and x != keep_id]

    if decision not in ("merge", "no_merge"):
        decision = "merge" if drop_ids else "no_merge"

    if decision == "no_merge":
        drop_ids = []

    try:
        if confidence is not None:
            confidence = float(confidence)
    except Exception:
        confidence = None

    return {
        "decision": decision,
        "keep_id": int(keep_id),
        "drop_ids": [int(x) for x in drop_ids],
        "reason": reason,
        "confidence": confidence
    }


# -------------------- 主流程 --------------------
def main():
    run_date = day_str(RUN_DATE)

    cand_log = os.path.join(LOG_DIR, f"candidate_build_log_{run_date}.txt")
    dedup_log = os.path.join(LOG_DIR, f"dedup_llm_log_{run_date}.txt")
    final_json_path = os.path.join(OUT_DIR, f"daily_dedup_final_{run_date}.json")

    conn = sqlite3.connect(DB_PATH)
    ensure_deduped_table_exists(conn)

    # ========== Step A：构建候选（内存） ==========
    articles = fetch_articles_for_day(conn, RUN_DATE)
    if not articles:
        conn.close()
        raise RuntimeError(f"当天 {run_date} 没查到文章（pub_time/fetched_at 都没命中）")

    emb_map = fetch_embeddings(conn, [a["id"] for a in articles])
    cand_result = build_candidate_groups(articles, emb_map, SIM_THRESHOLD)

    # 记录候选构建日志（轻量）
    with open(cand_log, "w", encoding="utf-8") as f:
        f.write(f"date={run_date}\nmodel={EMBED_MODEL}\nthreshold={SIM_THRESHOLD}\n")
        f.write(f"total={len(articles)} groups={len(cand_result['groups'])} isolated={len(cand_result['isolated_items'])} missing={len(cand_result['missing_embedding_articles'])}\n")
        f.write(f"generated_at={now_ts()}\n")
        f.write("\nTop pairs:\n")
        for p in cand_result.get("pairs_top", [])[:30]:
            f.write(f"- sim={p['sim']:.4f} {p['a_id']}<->{p['b_id']} | {p['a_title']} || {p['b_title']}\n")

    # 可选输出：JSON / DB / BOTH
    cand_json_path = None
    if CAND_OUTPUT.get("json"):
        cand_json_path = dump_candidate_json(cand_result, run_date)
    if CAND_OUTPUT.get("db"):
        write_candidate_db(conn, cand_result, run_date, clear=True)

    # ========== Step B：为 Dedup 获取候选输入 ==========
    if DEDUP_INPUT_MODE == "MEMORY":
        dedup_input = cand_result
    elif DEDUP_INPUT_MODE == "DB":
        dedup_input = load_candidate_from_db(conn, run_date)
    elif DEDUP_INPUT_MODE == "JSON":
        dedup_input = load_candidate_from_json(run_date)
    else:
        conn.close()
        raise RuntimeError(f"未知 DEDUP_INPUT_MODE={DEDUP_INPUT_MODE}")

    # ========== Step C：清空当天最终表（可选） ==========
    if DELETE_EXISTING_RUN_DATE:
        clear_deduped_run(conn, run_date)

    # ========== Step D：写入孤立 + 缺embedding（不调 LLM） ==========
    written: Set[int] = set()

    def write_solo_list(items: List[Dict[str, Any]], prefix: str, decision: str, reason: str):
        nonlocal written
        idx = 0
        for a in items:
            aid = int(a["id"])
            if aid in written:
                continue
            idx += 1
            gid = f"{prefix}{idx}"
            insert_deduped(conn, run_date, gid, a, decision, 1.0, reason)
            written.add(aid)
        conn.commit()
        return idx

    iso_written = write_solo_list(dedup_input.get("isolated_items", []), "iso", "solo", "无相似候选，直接保留")
    miss_written = write_solo_list(dedup_input.get("missing_embedding_articles", []), "miss", "solo", "缺embedding，直接保留（避免丢新闻）")

    # ========== Step E：处理 groups（每组调一次 LLM，写 keep） ==========
    decisions = []
    with open(dedup_log, "w", encoding="utf-8") as log_f:
        log_f.write(f"date={run_date}\nmodel={LLM_MODEL}\ninput_mode={DEDUP_INPUT_MODE}\n")
        log_f.write(f"groups={len(dedup_input.get('groups', []))} iso_written={iso_written} miss_written={miss_written}\n")
        log_f.write(f"generated_at={now_ts()}\n\n")

        for gi, g in enumerate(dedup_input.get("groups", []), start=1):
            group_id = g.get("group_id", f"g{gi}")

            # members 已经包含 content_text（MEMORY/JSON），DB 模式也会从 articles 拉好
            group_items = g.get("members", [])
            if len(group_items) < 2:
                continue

            log_f.write("=" * 92 + "\n")
            log_f.write(f"[{now_ts()}] Group {group_id} size={len(group_items)}\n")
            for it in group_items:
                log_f.write(f"- id={it['id']} ({it.get('source','')}) {it.get('pub_time','')} {it.get('title','')[:90]}\n")
            log_f.write("\n")

            messages = build_group_prompt(group_items)
            log_f.write(f"[{now_ts()}] ▶ REQUEST(system)\n{messages[0]['content']}\n\n")
            log_f.write(f"[{now_ts()}] ▶ REQUEST(user 前3500字)\n{messages[1]['content'][:3500]}\n\n")

            raw_path = os.path.join(RAW_DIR, f"llm_dedup_raw_{run_date}_{group_id}_{int(time.time())}.json")
            log_f.write(f"[{now_ts()}] ▶ calling LLM... raw_save={raw_path}\n")

            reply = call_llm(messages, raw_path)
            log_f.write(f"[{now_ts()}] ◀ RESPONSE(raw)\n{reply}\n\n")

            parsed = json.loads(extract_first_json(reply))
            norm = normalize_decision(parsed, group_items)

            keep_id = norm["keep_id"]
            decision = norm["decision"]
            drop_ids = norm["drop_ids"]
            reason = norm["reason"]
            confidence = norm["confidence"]

            # 写 keep
            keep_article = None
            for it in group_items:
                if int(it["id"]) == keep_id:
                    keep_article = it
                    break
            if keep_article is None:
                keep_article = group_items[0]
                keep_id = int(keep_article["id"])

            if keep_id not in written:
                insert_deduped(conn, run_date, group_id, keep_article, decision, confidence, reason)
                conn.commit()
                written.add(keep_id)

            decisions.append({
                "group_id": group_id,
                "member_ids": g.get("member_ids", []),
                "decision": decision,
                "keep_id": keep_id,
                "drop_ids": drop_ids,
                "reason": reason,
                "confidence": confidence
            })

            log_f.write(f"[{now_ts()}] ✅ PARSED: decision={decision} keep_id={keep_id} drop_ids={drop_ids}\n")
            log_f.write(f"[{now_ts()}] ✅ DB WRITE: {DEDUPED_TABLE} group_id={group_id} keep_id={keep_id}\n")
            log_f.write("-" * 92 + "\n\n")

            time.sleep(SLEEP_SECONDS_BETWEEN_CALLS)

        # 汇总
        cnt = conn.execute(f"SELECT COUNT(*) FROM {DEDUPED_TABLE} WHERE dedup_run_date = ?", (run_date,)).fetchone()[0]
        log_f.write("=" * 92 + "\n")
        log_f.write(f"[{now_ts()}] SUMMARY\n")
        log_f.write(f"written_unique={len(written)} db_count={cnt}\n")

    # ========== Step F：可选导出最终 JSON ==========
    if EXPORT_FINAL_JSON:
        rows = conn.execute(
            f"""
            SELECT dedup_group_id, article_id, title, url, source, pub_time, decision, confidence, reason
            FROM {DEDUPED_TABLE}
            WHERE dedup_run_date = ?
            ORDER BY pub_time DESC
            """,
            (run_date,)
        ).fetchall()

        items = []
        for r in rows:
            items.append({
                "dedup_group_id": r[0],
                "article_id": r[1],
                "title": r[2],
                "url": r[3],
                "source": r[4],
                "pub_time": r[5],
                "decision": r[6],
                "confidence": r[7],
                "reason": r[8],
            })

        final_out = {
            "date": run_date,
            "candidate_output": {"json": bool(CAND_OUTPUT.get("json")), "db": bool(CAND_OUTPUT.get("db"))},
            "candidate_json_path": cand_json_path,
            "dedup_input_mode": DEDUP_INPUT_MODE,
            "sim_threshold": SIM_THRESHOLD,
            "embed_model": EMBED_MODEL,
            "llm_model": LLM_MODEL,
            "count_final": len(items),
            "items": items,
            "logs": {"candidate": cand_log, "dedup": dedup_log},
        }

        with open(final_json_path, "w", encoding="utf-8") as f:
            json.dump(final_out, f, ensure_ascii=False, indent=2)

    conn.close()

    print(f"✅ pipeline 完成 run_date={run_date}")
    print(f"✅ 候选日志：{cand_log}")
    if CAND_OUTPUT.get("json"):
        print(f"✅ 候选JSON：{os.path.join(OUT_DIR, f'candidate_groups_{run_date}.json')}")
    if CAND_OUTPUT.get("db"):
        print(f"✅ 候选已写DB表：{CAND_TABLE} (run_date={run_date})")
    print(f"✅ 去重LLM日志：{dedup_log}")
    print(f"✅ 最终写入：{DEDUPED_TABLE} (run_date={run_date})")
    if EXPORT_FINAL_JSON:
        print(f"✅ 最终导出：{final_json_path}")


if __name__ == "__main__":
    main()
