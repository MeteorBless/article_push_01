# -*- coding: utf-8 -*-
"""
pipeline_daily_dedup.py

目标（按你最新要求）：
- 保留 AUTO 版本：候选读取顺序 = MEMORY -> DB -> JSON -> BUILD
- 同时保证：一旦当天 articles 发生变化（新增/删减/变化），自动重建候选并覆盖写回（DB + 可选 JSON）
- 候选输出：可选写 DB / 写 JSON / BOTH
- 最终写入：articles_deduped（含 isolated/missing 直接保留）
- JSON 文件统一写入 json/ 目录（包含 candidate_groups_*.json、daily_dedup_final_*.json、LLM raw json）

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


# =========================
# 配置区（你只需要改这里）
# =========================
DB_PATH = "news.db"

TODAY = datetime.date.today()
RUN_DATE = TODAY - datetime.timedelta(days=1)
# RUN_DATE = datetime.date(2026, 1, 29)  # 手动指定

# embedding 配置
EMBED_MODEL = "Qwen/Qwen3-Embedding-4B"
SIM_THRESHOLD = 0.85

# 跨天去重（滚动窗口）：把“今天文章”与最近N天已保留主条做相似度比对，命中则过滤掉今天的重复稿
ENABLE_CROSSDAY_DEDUP = True
HISTORY_DAYS = 7
# 跨天阈值建议比当天更严格一些（避免误杀不同事件的相似稿）
CROSSDAY_SIM_THRESHOLD = 0.88

# 候选组持久化开关：可选 JSON / DB / BOTH
CAND_OUTPUT = {
    "json": True,
    "db": True
}

# 自动切换读取候选组顺序（核心）
AUTO_CAND_LOAD_ORDER = ["MEMORY", "DB", "JSON", "BUILD"]

# candidate_groups 表名与 JSON 文件模板
CAND_TABLE = "candidate_groups"
CANDIDATE_JSON_TEMPLATE = "candidate_groups_{date}.json"  # 会写入 JSON_DIR

# LLM（硅基流动，OpenAI兼容接口）
LLM_URL = "https://api.siliconflow.cn/v1/chat/completions"
LLM_MODEL = "deepseek-ai/DeepSeek-V3.2"

# 你自己写死的 Key（照你原样保留）
SILICONFLOW_API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"

if not SILICONFLOW_API_KEY:
    raise RuntimeError("Missing SILICONFLOW_API_KEY env var (used by news_writeback.py).")

TEMPERATURE = 0.2
TIMEOUT_SECONDS = 90
SLEEP_SECONDS_BETWEEN_CALLS = 1

# 给模型看的正文预览长度
CONTENT_PREVIEW_LEN = 1800

# 最终结果表（你说已经建好了）
DEDUPED_TABLE = "articles_deduped"

# 每次跑是否先清空当天 dedup 结果（避免重复）
DELETE_EXISTING_RUN_DATE = True

# 输出目录
OUT_DIR = "."          # 文字日志/其他输出根目录
LOG_DIR = "logs"       # txt 日志目录
JSON_DIR = "json"      # ✅ 所有 JSON 输出目录（候选/最终/LLM raw）
RAW_DIR = os.path.join(LOG_DIR, "llm_raw_logs")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# 是否导出去重后的最终JSON（可选）
EXPORT_FINAL_JSON = True
# =========================


# =========================
# 工具函数
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
        raise ValueError(f"无法从模型输出提取JSON，输出前200字：{text[:200]}")
    return m.group(0)


def _ids_signature(article_ids: List[int]) -> str:
    """
    用于判断“当天 articles 集合是否变化”。
    这里用集合的 n/min/max 做轻量签名；并且我们会做“严格集合相等”校验（更稳）。
    """
    if not article_ids:
        return "empty"
    s = sorted(set(int(x) for x in article_ids))
    return f"n={len(s)};min={s[0]};max={s[-1]}"


# =========================
# DB读取：articles / embeddings
# =========================
def fetch_articles_for_day(conn: sqlite3.Connection, d: datetime.date) -> List[Dict[str, Any]]:
    """
    优先按 pub_time LIKE 'YYYY-MM-DD%' 取当天；取不到则用 fetched_at 兜底
    """
    ds = day_str(d)

    rows = conn.execute(
        """
        SELECT id,
               COALESCE(title,'')        AS title,
               COALESCE(url,'')          AS url,
               COALESCE(source,'')       AS source,
               COALESCE(pub_time,'')     AS pub_time,
               COALESCE(content_text,'') AS content_text
        FROM articles
        WHERE pub_time LIKE ?
        ORDER BY pub_time DESC, id DESC
        """,
        (f"{ds}%",)
    ).fetchall()

    if not rows:
        rows = conn.execute(
            """
            SELECT id,
                   COALESCE(title,'')        AS title,
                   COALESCE(url,'')          AS url,
                   COALESCE(source,'')       AS source,
                   COALESCE(pub_time,'')     AS pub_time,
                   COALESCE(content_text,'') AS content_text
            FROM articles
            WHERE fetched_at >= ? AND fetched_at <= ?
            ORDER BY fetched_at DESC, id DESC
            """,
            (f"{ds} 00:00:00", f"{ds} 23:59:59")
        ).fetchall()

    items = []
    for r in rows:
        items.append({
            "article_id": int(r[0]),
            "id": int(r[0]),  # deprecated alias
            "title": clean_text(r[1]),
            "url": r[2],
            "source": r[3],
            "pub_time": r[4],
            "content_text": r[5] or "",
        })
    return items


def fetch_articles_by_ids(conn: sqlite3.Connection, ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    q_marks = ",".join(["?"] * len(ids))
    rows = conn.execute(
        f"""
        SELECT id,
               COALESCE(title,'')        AS title,
               COALESCE(url,'')          AS url,
               COALESCE(source,'')       AS source,
               COALESCE(pub_time,'')     AS pub_time,
               COALESCE(content_text,'') AS content_text
        FROM articles
        WHERE id IN ({q_marks})
        """,
        ids
    ).fetchall()

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        aid = int(r[0])
        out[aid] = {
            "article_id": aid,
            "id": aid,  # deprecated alias
            "title": clean_text(r[1]),
            "url": r[2],
            "source": r[3],
            "pub_time": r[4],
            "content_text": r[5] or "",
        }
    return out

# =========================
# 跨天滚动去重（窗口历史主条）
# =========================
def fetch_history_deduped_article_ids(conn: sqlite3.Connection, target_day: datetime.date, history_days: int) -> List[int]:
    """
    取 target_day 之前 history_days 天内已经写入 articles_deduped 的主条 article_id。
    注意：这里用 dedup_run_date 作为“批次日期”，不是文章 pub_time。
    """
    if history_days <= 0:
        return []
    start_day = target_day - datetime.timedelta(days=history_days)
    end_day = target_day - datetime.timedelta(days=1)
    if end_day < start_day:
        return []

    start_s = day_str(start_day)
    end_s = day_str(end_day)

    rows = conn.execute(
        f"""
        SELECT DISTINCT article_id
        FROM {DEDUPED_TABLE}
        WHERE dedup_run_date >= ? AND dedup_run_date <= ?
        """,
        (start_s, end_s)
    ).fetchall()
    return [int(r[0]) for r in rows]


def filter_crossday_duplicates(
    conn: sqlite3.Connection,
    articles: List[Dict[str, Any]],
    emb_map: Dict[int, np.ndarray],
    target_day: datetime.date,
    history_days: int,
    sim_threshold: float
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    用“今天文章 embedding”对比“历史窗口内已保留主条 embedding”，命中阈值则过滤掉今天的重复稿。

    返回：
    - kept_articles: 过滤后的文章列表（供当天候选组/LLM去重继续处理）
    - dropped: 详细记录（用于日志/审计/最终json）
    """
    if not articles or not emb_map:
        return articles, []

    hist_ids = fetch_history_deduped_article_ids(conn, target_day, history_days)
    if not hist_ids:
        return articles, []

    hist_emb = fetch_embeddings(conn, hist_ids)
    if not hist_emb:
        return articles, []

    # today vectors（只对有embedding的做跨天比对；缺embedding的不过滤，避免误丢）
    today_with_emb = [a for a in articles if int(a.get("article_id")) in emb_map]
    if not today_with_emb:
        return articles, []

    today_ids = [int(a["article_id"]) for a in today_with_emb]
    hist_ids2 = [hid for hid in hist_ids if hid in hist_emb]
    if not hist_ids2:
        return articles, []

    tv = np.stack([emb_map[tid] for tid in today_ids], axis=0).astype(np.float32)
    hv = np.stack([hist_emb[hid] for hid in hist_ids2], axis=0).astype(np.float32)

    # normalize -> cosine
    tv_norm = tv / np.clip(np.linalg.norm(tv, axis=1, keepdims=True), 1e-12, None)
    hv_norm = hv / np.clip(np.linalg.norm(hv, axis=1, keepdims=True), 1e-12, None)
    sims = np.matmul(tv_norm, hv_norm.T)  # [today, hist]

    best_j = np.argmax(sims, axis=1)
    best_sim = np.max(sims, axis=1)

    # 补充历史文章信息（用于日志）
    keep_ids = sorted({int(hist_ids2[j]) for j, s in zip(best_j, best_sim) if float(s) >= sim_threshold})
    keep_info = fetch_articles_by_ids(conn, keep_ids)

    dropped: List[Dict[str, Any]] = []
    drop_id_set: Set[int] = set()

    for i, tid in enumerate(today_ids):
        s = float(best_sim[i])
        if s < sim_threshold:
            continue
        kid = int(hist_ids2[int(best_j[i])])

        a = next((x for x in today_with_emb if int(x["article_id"]) == tid), None)
        if not a:
            continue

        k = keep_info.get(kid, {})
        dropped.append({
            "drop_id": tid,
            "drop_title": a.get("title", ""),
            "drop_source": a.get("source", ""),
            "drop_pub_time": a.get("pub_time", ""),
            "keep_id": kid,
            "keep_title": k.get("title", ""),
            "keep_source": k.get("source", ""),
            "keep_pub_time": k.get("pub_time", ""),
            "sim": round(s, 4),
            "reason": f"crossday_sim>={sim_threshold} (history_days={history_days})"
        })
        drop_id_set.add(tid)

    if not drop_id_set:
        return articles, []

    kept_articles = [a for a in articles if int(a.get("article_id")) not in drop_id_set]
    return kept_articles, dropped



def fetch_embeddings(conn: sqlite3.Connection, article_ids: List[int]) -> Dict[int, np.ndarray]:
    """
    从 article_embeddings 表加载 embedding（json数组）
    """
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


# =========================
# 纯逻辑：构建候选组（不做IO）
# =========================
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
    输出：
    - groups: size>=2 的连通分量
    - isolated_items: size==1 且无边的文章（有embedding）
    - missing_embedding_articles: 没 embedding 的文章
    - pairs_top: 用于日志调参（最高相似的前N对）
    """
    kept = [a for a in articles if a["article_id"] in emb_map]
    missing = [a for a in articles if a["article_id"] not in emb_map]

    if len(kept) < 2:
        return {
            "groups": [],
            "isolated_items": kept,
            "missing_embedding_articles": missing,
            "pairs_top": [],
        }

    vectors = np.stack([emb_map[a["article_id"]] for a in kept], axis=0)
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
            "member_ids": [m["article_id"] for m in members_sorted],
            "members": members_sorted,
        })

    isolated_items = [kept[idx] for idx in isolated_idx]

    pairs_top = sorted(pairs, key=lambda x: x[2], reverse=True)[:200]
    pairs_top_out = [
        {
            "a_id": kept[i]["article_id"],
            "b_id": kept[j]["article_id"],
            "sim": s,
            "a_title": kept[i]["title"][:120],
            "b_title": kept[j]["title"][:120],
        } for i, j, s in pairs_top
    ]

    return {
        "groups": groups,
        "isolated_items": isolated_items,
        "missing_embedding_articles": missing,
        "pairs_top": pairs_top_out,
    }


# =========================
# IO：candidate_groups 写 DB / JSON
# =========================
def ensure_candidate_table(conn: sqlite3.Connection):
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


def candidate_db_has_run_date(conn: sqlite3.Connection, run_date: str) -> bool:
    ensure_candidate_table(conn)
    row = conn.execute(
        f"SELECT 1 FROM {CAND_TABLE} WHERE run_date = ? LIMIT 1",
        (run_date,)
    ).fetchone()
    return row is not None


def write_candidate_db(conn: sqlite3.Connection, result: Dict[str, Any], run_date: str, clear: bool = True):
    ensure_candidate_table(conn)
    if clear:
        conn.execute(f"DELETE FROM {CAND_TABLE} WHERE run_date = ?", (run_date,))
        conn.commit()

    ts = now_ts()

    # groups
    for g in result.get("groups", []):
        gid = g.get("group_id", "")
        for aid in g.get("member_ids", []):
            conn.execute(
                f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
                (run_date, "group", gid, int(aid), ts)
            )

    # isolated
    for a in result.get("isolated_items", []):
        conn.execute(
            f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
            (run_date, "isolated", "iso", int(a["article_id"]), ts)
        )

    # missing
    for a in result.get("missing_embedding_articles", []):
        conn.execute(
            f"INSERT OR REPLACE INTO {CAND_TABLE} (run_date, kind, group_id, article_id, created_at) VALUES (?,?,?,?,?)",
            (run_date, "missing", "miss", int(a["article_id"]), ts)
        )

    conn.commit()


def _candidate_ids_from_result(res: Dict[str, Any]) -> Set[int]:
    ids: Set[int] = set()
    for g in (res.get("groups") or []):
        for aid in (g.get("member_ids") or []):
            ids.add(int(aid))
    for a in (res.get("isolated_items") or []):
        if "id" in a:
            ids.add(int(a["article_id"]))
    for a in (res.get("missing_embedding_articles") or []):
        if "id" in a:
            ids.add(int(a["article_id"]))
    return ids


def _candidate_ids_from_db(conn: sqlite3.Connection, run_date: str) -> Set[int]:
    ensure_candidate_table(conn)
    rows = conn.execute(
        f"SELECT article_id FROM {CAND_TABLE} WHERE run_date = ?",
        (run_date,)
    ).fetchall()
    return {int(r[0]) for r in rows}


def _db_candidate_is_fresh(conn: sqlite3.Connection, run_date: str, current_article_ids: List[int]) -> bool:
    current = set(int(x) for x in current_article_ids)
    existed = _candidate_ids_from_db(conn, run_date)
    return existed == current


def dump_candidate_json(result: Dict[str, Any], run_date: str) -> str:
    # ✅ JSON 写入 json/ 目录
    path = os.path.join(JSON_DIR, CANDIDATE_JSON_TEMPLATE.format(date=run_date))
    out = {
        "date": run_date,
        "model": EMBED_MODEL,
        "threshold": SIM_THRESHOLD,
        **result
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    return path


def candidate_json_exists(run_date: str) -> str:
    path = os.path.join(JSON_DIR, CANDIDATE_JSON_TEMPLATE.format(date=run_date))
    return path if os.path.exists(path) else ""


def load_candidate_from_json(run_date: str) -> Dict[str, Any]:
    path = os.path.join(JSON_DIR, CANDIDATE_JSON_TEMPLATE.format(date=run_date))
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    return {
        "groups": obj.get("groups", []) or [],
        "isolated_items": obj.get("isolated_items", []) or [],
        "missing_embedding_articles": obj.get("missing_embedding_articles", []) or [],
        "pairs_top": obj.get("pairs_top", []) or []
    }


def _json_candidate_is_fresh(run_date: str, current_article_ids: List[int]) -> bool:
    p = candidate_json_exists(run_date)
    if not p:
        return False
    try:
        res = load_candidate_from_json(run_date)
        existed = _candidate_ids_from_result(res)
        current = set(int(x) for x in current_article_ids)
        return existed == current
    except Exception:
        return False


def load_candidate_from_db(conn: sqlite3.Connection, run_date: str) -> Dict[str, Any]:
    ensure_candidate_table(conn)

    rows = conn.execute(
        f"SELECT kind, group_id, article_id FROM {CAND_TABLE} WHERE run_date = ?",
        (run_date,)
    ).fetchall()

    group_map: Dict[str, List[int]] = {}
    isolated_ids: List[int] = []
    missing_ids: List[int] = []

    for kind, gid, aid in rows:
        aid = int(aid)
        if kind == "group":
            group_map.setdefault(gid, []).append(aid)
        elif kind == "isolated":
            isolated_ids.append(aid)
        elif kind == "missing":
            missing_ids.append(aid)

    all_ids = sorted(set([*isolated_ids, *missing_ids, *[x for ids in group_map.values() for x in ids]]))
    id_to_article = fetch_articles_by_ids(conn, all_ids)

    groups = []
    for gid, ids in sorted(group_map.items(), key=lambda x: x[0]):
        members = [id_to_article[i] for i in ids if i in id_to_article]
        members_sorted = sorted(members, key=lambda x: x.get("pub_time") or "")
        groups.append({
            "group_id": gid,
            "member_ids": [m["article_id"] for m in members_sorted],
            "members": members_sorted
        })

    isolated_items = [id_to_article[i] for i in isolated_ids if i in id_to_article]
    missing_items = [id_to_article[i] for i in missing_ids if i in id_to_article]

    return {
        "groups": groups,
        "isolated_items": isolated_items,
        "missing_embedding_articles": missing_items,
        "pairs_top": []
    }


def _ensure_outputs_synced(
    conn: sqlite3.Connection,
    run_date: str,
    res: Dict[str, Any],
    current_article_ids: List[int],
):
    """
    保证：如果 CAND_OUTPUT 打开了 DB/JSON，就让两者都“存在且是最新覆盖”。
    - DB：若不新鲜则覆盖写（clear=True）
    - JSON：直接覆盖写（同名覆盖）
    """
    if CAND_OUTPUT.get("db"):
        if not _db_candidate_is_fresh(conn, run_date, current_article_ids):
            write_candidate_db(conn, res, run_date, clear=True)

    if CAND_OUTPUT.get("json"):
        dump_candidate_json(res, run_date)


def get_candidate_result_auto(
    conn: sqlite3.Connection,
    run_date: str,
    *,
    articles: Optional[List[Dict[str, Any]]] = None,
    emb_map: Optional[Dict[int, np.ndarray]] = None,
) -> Tuple[Dict[str, Any], str]:
    """
    AUTO：MEMORY -> DB -> JSON -> BUILD
    并保证：只要当天 articles 集合变化（新增/删减/变化），就强制 BUILD 并覆盖写回（DB/JSON）。
    返回：(candidate_result, source_tag)
    """
    current_article_ids = [int(a["article_id"]) for a in (articles or [])]
    current_sig = _ids_signature(current_article_ids)

    # 1) MEMORY：必须 run_date + sig 一致才命中
    if "MEMORY" in AUTO_CAND_LOAD_ORDER:
        cache = getattr(get_candidate_result_auto, "_cache", None)
        if cache and cache.get("run_date") == run_date and cache.get("sig") == current_sig and cache.get("result"):
            res = cache["result"]
            _ensure_outputs_synced(conn, run_date, res, current_article_ids)
            return res, "MEMORY"

    # 2) DB：有数据且“覆盖集合一致”才算新鲜，否则视为过期
    if "DB" in AUTO_CAND_LOAD_ORDER:
        try:
            if candidate_db_has_run_date(conn, run_date) and _db_candidate_is_fresh(conn, run_date, current_article_ids):
                res = load_candidate_from_db(conn, run_date)
                get_candidate_result_auto._cache = {"run_date": run_date, "sig": current_sig, "result": res}
                _ensure_outputs_synced(conn, run_date, res, current_article_ids)
                return res, "DB"
        except Exception:
            pass

    # 3) JSON：存在且“覆盖集合一致”才算新鲜，否则视为过期
    if "JSON" in AUTO_CAND_LOAD_ORDER:
        try:
            if _json_candidate_is_fresh(run_date, current_article_ids):
                res = load_candidate_from_json(run_date)
                get_candidate_result_auto._cache = {"run_date": run_date, "sig": current_sig, "result": res}
                _ensure_outputs_synced(conn, run_date, res, current_article_ids)
                return res, "JSON"
        except Exception:
            pass

    # 4) BUILD：DB/JSON/MEMORY 不可用或已过期 → 重建并覆盖写
    if "BUILD" in AUTO_CAND_LOAD_ORDER:
        if articles is None or emb_map is None:
            raise RuntimeError("AUTO 走到 BUILD，但未传入 articles/emb_map，无法构建候选组。")

        res = build_candidate_groups(articles, emb_map, SIM_THRESHOLD)
        # 覆盖写回（按开关）
        if CAND_OUTPUT.get("db"):
            write_candidate_db(conn, res, run_date, clear=True)
        if CAND_OUTPUT.get("json"):
            dump_candidate_json(res, run_date)

        get_candidate_result_auto._cache = {"run_date": run_date, "sig": current_sig, "result": res}
        return res, "BUILD"

    raise RuntimeError("AUTO_CAND_LOAD_ORDER 未包含可用路径（MEMORY/DB/JSON/BUILD）。")


# =========================
# DB：articles_deduped 写入
# =========================
def ensure_deduped_table_exists(conn: sqlite3.Connection):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (DEDUPED_TABLE,)
    ).fetchone()
    if not row:
        raise RuntimeError(f"数据库中不存在表 {DEDUPED_TABLE}（请确认你操作的是同一个 news.db）")


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
            dedup_run_date, dedup_group_id, article_id,
            title, url, source, pub_time, content_text,
            decision, confidence, reason, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_date,
            group_id,
            int(article["article_id"]),
            article.get("title", "") or "",
            article.get("url", "") or "",
            article.get("source", "") or "",
            article.get("pub_time", "") or "",
            article.get("content_text", "") or "",
            decision,
            confidence,
            reason,
            now_ts()
        )
    )


# =========================
# LLM 去重：每组调用一次
# =========================
def call_llm(messages: List[Dict[str, str]], raw_save_path: str) -> str:
    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": TEMPERATURE
    }

    resp = requests.post(LLM_URL, headers=headers, json=payload, timeout=TIMEOUT_SECONDS)

    # 保存原始响应（成功/失败都存）
    try:
        raw_obj = resp.json()
    except Exception:
        raw_obj = {"status_code": resp.status_code, "text": resp.text}

    with open(raw_save_path, "w", encoding="utf-8") as f:
        json.dump(raw_obj, f, ensure_ascii=False, indent=2)

    if resp.status_code != 200:
        raise RuntimeError(f"LLM 调用失败 HTTP {resp.status_code}：{resp.text[:2000]}")

    return raw_obj["choices"][0]["message"]["content"]


def build_group_prompt(group_items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    system = "你是新闻语义去重裁判。你必须只输出严格JSON，不能输出任何额外文字或markdown。"

    payload = {
        "task": "dedup_and_pick_canonical",
        "rules": [
            "同一事件/同一会议/同一政策/同一讲话/同一文件/同一通稿（即使标题不同、来源不同）可判为重复。",
            "如果只是同主题但报道不同细节，宁可不合并（保守）。",
            "选择主条 keep_id：信息更完整、表述更权威、更像原稿；可参考来源权威性与发布时间。"
        ],
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
            "id": str(it["article_id"]),
            "title": it.get("title", ""),
            "source": it.get("source", ""),
            "pub_time": it.get("pub_time", ""),
            "url": it.get("url", ""),
            "content_preview": (it.get("content_text") or "")[:CONTENT_PREVIEW_LEN]
        })

    user = (
        "请对同一候选组新闻做最终去重判定：\n"
        "- 若应合并：decision='merge'，keep_id 为主条，drop_ids 为重复条。\n"
        "- 若不应合并：decision='no_merge'，drop_ids 必须为空；keep_id 任取其一。\n"
        "必须只输出严格JSON，且 keep_id/drop_ids 必须来自 items。\n\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def normalize_decision(parsed: Dict[str, Any], group_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    valid_ids = {str(it["article_id"]) for it in group_items}

    keep_id = str(parsed.get("keep_id", "")).strip()
    drop_ids = [str(x).strip() for x in (parsed.get("drop_ids") or [])]
    decision = (parsed.get("decision") or "").strip()
    reason = (parsed.get("reason") or "").strip()
    confidence = parsed.get("confidence", None)

    if keep_id not in valid_ids:
        keep_id = str(group_items[0]["article_id"])

    drop_ids = [x for x in drop_ids if x in valid_ids and x != keep_id]

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


# =========================
# 主流程
# =========================
def main():
    run_date = day_str(RUN_DATE)

    candidate_log_path = os.path.join(LOG_DIR, f"candidate_build_log_{run_date}.txt")
    dedup_log_path = os.path.join(LOG_DIR, f"dedup_llm_log_{run_date}.txt")
    # ✅ 最终 JSON 写入 json/ 目录
    final_json_path = os.path.join(JSON_DIR, f"daily_dedup_final_{run_date}.json")

    conn = sqlite3.connect(DB_PATH)
    ensure_deduped_table_exists(conn)

    # A) 拉当天文章 + embedding（供 BUILD 使用，也用于 freshness 判断）
    articles = fetch_articles_for_day(conn, RUN_DATE)
    if not articles:
        conn.close()
        raise RuntimeError(f"当天 {run_date} 没查到文章（pub_time/fetched_at 都没命中）")

    emb_map = fetch_embeddings(conn, [a["article_id"] for a in articles])

    # A2) 跨天滚动去重（可选）：过滤掉“与历史窗口内主条高度相似”的今天重复稿
    crossday_dropped: List[Dict[str, Any]] = []
    original_count = len(articles)

    if ENABLE_CROSSDAY_DEDUP:
        articles, crossday_dropped = filter_crossday_duplicates(
            conn=conn,
            articles=articles,
            emb_map=emb_map,
            target_day=RUN_DATE,
            history_days=HISTORY_DAYS,
            sim_threshold=CROSSDAY_SIM_THRESHOLD
        )

    # 如果跨天过滤后当天文章为空，直接退出（说明今天全是历史重复或抓取异常）
    if not articles:
        conn.close()
        raise RuntimeError(
            f"run_date={run_date} 跨天过滤后无可处理文章：original={original_count}, dropped_crossday={len(crossday_dropped)}"
        )

    # 过滤后，也同步缩小 emb_map（避免后续误统计）
    emb_map = fetch_embeddings(conn, [a["article_id"] for a in articles])


    # B) AUTO 获取候选组（MEMORY -> DB -> JSON -> BUILD），并保证“变了就重建覆盖写回”
    cand_result, cand_source = get_candidate_result_auto(
        conn,
        run_date,
        articles=articles,
        emb_map=emb_map
    )

    # C) 候选日志（你能看到到底从哪读的 + 基本统计 + top pairs）
    with open(candidate_log_path, "w", encoding="utf-8") as f:
        f.write(f"=== Candidate Build ===\n")
        f.write(f"run_date={run_date}\n")
        f.write(f"cand_source={cand_source}\n")
        f.write(f"embed_model={EMBED_MODEL}\n")
        f.write(f"threshold={SIM_THRESHOLD}\n")
        f.write(f"total_articles_after_crossday_filter={len(articles)}\n")
        f.write(f"total_articles_before_crossday_filter={original_count}\n")
        f.write(f"crossday_dropped={len(crossday_dropped)}\n")
        f.write(f"with_embedding={len([a for a in articles if a['article_id'] in emb_map])}\n")
        f.write(f"missing_embedding={len([a for a in articles if a['article_id'] not in emb_map])}\n")
        f.write(f"groups={len(cand_result.get('groups', []))}\n")
        f.write(f"isolated={len(cand_result.get('isolated_items', []))}\n")
        f.write(f"missing_items={len(cand_result.get('missing_embedding_articles', []))}\n")
        f.write(f"generated_at={now_ts()}\n\n")
        f.write("---- Top Similar Pairs (for tuning) ----\n")
        for p in (cand_result.get("pairs_top", []) or [])[:50]:
            f.write(f"- sim={p['sim']:.4f} {p['a_id']}<->{p['b_id']} | {p['a_title']} || {p['b_title']}\n")

        if crossday_dropped:
            f.write("\n---- Crossday Dropped (filtered before intra-day dedup) ----\n")
            for d in crossday_dropped:
                f.write(
                    f"- sim={d['sim']:.4f} drop={d['drop_id']} {d.get('drop_pub_time','')} {d.get('drop_title','')[:90]} "
                    f"|| keep={d['keep_id']} {d.get('keep_pub_time','')} {d.get('keep_title','')[:90]}\n"
                )


    # D) 去重结果表：清空当天（可选）
    if DELETE_EXISTING_RUN_DATE:
        clear_deduped_run(conn, run_date)

    written: Set[int] = set()
    decisions: List[Dict[str, Any]] = []

    # E) 先写入 isolated / missing（不调LLM）
    def write_solo(items: List[Dict[str, Any]], prefix: str, reason: str) -> int:
        nonlocal written
        idx = 0
        for a in items:
            aid = int(a["article_id"])
            if aid in written:
                continue
            idx += 1
            insert_deduped(
                conn=conn,
                run_date=run_date,
                group_id=f"{prefix}{idx}",
                article=a,
                decision="solo",
                confidence=1.0,
                reason=reason
            )
            written.add(aid)
        conn.commit()
        return idx

    iso_written = write_solo(cand_result.get("isolated_items", []) or [], "iso", "无相似候选，直接保留")
    miss_written = write_solo(cand_result.get("missing_embedding_articles", []) or [], "miss", "缺embedding，直接保留（避免丢新闻）")

    # F) 处理 groups：每组调一次 LLM，写 keep
    with open(dedup_log_path, "w", encoding="utf-8") as log_f:
        log_f.write(f"=== LLM Dedup ===\n")
        log_f.write(f"run_date={run_date}\n")
        log_f.write(f"cand_source={cand_source}\n")
        log_f.write(f"llm_model={LLM_MODEL}\n")
        log_f.write(f"groups={len(cand_result.get('groups', []))}\n")
        log_f.write(f"isolated_written={iso_written} missing_written={miss_written}\n")
        log_f.write(f"generated_at={now_ts()}\n\n")

        for gi, g in enumerate(cand_result.get("groups", []) or [], start=1):
            group_id = g.get("group_id", f"g{gi}")
            group_items = g.get("members", []) or []

            if len(group_items) < 2:
                continue

            log_f.write("=" * 92 + "\n")
            log_f.write(f"[{now_ts()}] Group {group_id} size={len(group_items)}\n")
            for it in group_items:
                log_f.write(f"- id={it['article_id']} ({it.get('source','')}) {it.get('pub_time','')} {it.get('title','')[:90]}\n")
            log_f.write("\n")

            messages = build_group_prompt(group_items)

            # 记录请求
            log_f.write(f"[{now_ts()}] ▶ REQUEST(system)\n{messages[0]['content']}\n\n")
            log_f.write(f"[{now_ts()}] ▶ REQUEST(user 前3500字)\n{messages[1]['content'][:3500]}\n\n")

            # ✅ raw json 写入 json/llm_raw_logs
            raw_path = os.path.join(RAW_DIR, f"llm_dedup_raw_{run_date}_{group_id}_{int(time.time())}.json")
            log_f.write(f"[{now_ts()}] ▶ calling LLM... raw_save={raw_path}\n")

            reply = call_llm(messages, raw_path)

            log_f.write(f"[{now_ts()}] ◀ RESPONSE(raw)\n{reply}\n\n")

            parsed = json.loads(extract_first_json(reply))
            norm = normalize_decision(parsed, group_items)

            decision = norm["decision"]
            keep_id = norm["keep_id"]
            drop_ids = norm["drop_ids"]
            reason = norm["reason"]
            confidence = norm["confidence"]

            # 找 keep 文章对象
            keep_article = None
            for it in group_items:
                if int(it["article_id"]) == keep_id:
                    keep_article = it
                    break
            if keep_article is None:
                keep_article = group_items[0]
                keep_id = int(keep_article["article_id"])

            # 写入 keep（只写主条）
            if keep_id not in written:
                insert_deduped(
                    conn=conn,
                    run_date=run_date,
                    group_id=group_id,
                    article=keep_article,
                    decision=decision,
                    confidence=confidence,
                    reason=reason
                )
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

        cnt = conn.execute(
            f"SELECT COUNT(*) FROM {DEDUPED_TABLE} WHERE dedup_run_date = ?",
            (run_date,)
        ).fetchone()[0]

        log_f.write("=" * 92 + "\n")
        log_f.write(f"[{now_ts()}] SUMMARY\n")
        log_f.write(f"written_unique={len(written)} db_count={cnt}\n")

    # G) 导出最终 JSON（可选，写入 json/）
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

        out_items = []
        for r in rows:
            out_items.append({
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

        candidate_json_path = os.path.join(JSON_DIR, CANDIDATE_JSON_TEMPLATE.format(date=run_date)) \
            if CAND_OUTPUT.get("json") else ""

        final_out = {
            "date": run_date,
            "candidate_source": cand_source,
            "candidate_output": {"json": bool(CAND_OUTPUT.get("json")), "db": bool(CAND_OUTPUT.get("db"))},
            "auto_load_order": AUTO_CAND_LOAD_ORDER,
            "candidate_json_path": candidate_json_path,
            "embed_model": EMBED_MODEL,
            "sim_threshold": SIM_THRESHOLD,
            "crossday_dedup": {
                "enabled": bool(ENABLE_CROSSDAY_DEDUP),
                "history_days": int(HISTORY_DAYS),
                "threshold": float(CROSSDAY_SIM_THRESHOLD),
                "dropped_count": len(crossday_dropped),
                "dropped": crossday_dropped
            },
            "llm_model": LLM_MODEL,
            "count_final": len(out_items),
            "items": out_items,
            "decisions": decisions,
            "logs": {
                "candidate": candidate_log_path,
                "dedup": dedup_log_path
            }
        }

        with open(final_json_path, "w", encoding="utf-8") as f:
            json.dump(final_out, f, ensure_ascii=False, indent=2)

    conn.close()

    print(f"✅ pipeline 完成 run_date={run_date}")
    print(f"✅ 候选来源：{cand_source}（AUTO顺序：{AUTO_CAND_LOAD_ORDER}，且变更会自动重建覆盖）")
    print(f"✅ 候选日志：{candidate_log_path}")
    if CAND_OUTPUT.get("json"):
        print(f"✅ 候选JSON：{os.path.join(JSON_DIR, CANDIDATE_JSON_TEMPLATE.format(date=run_date))}")
    if CAND_OUTPUT.get("db"):
        print(f"✅ 候选DB表：{CAND_TABLE}（run_date={run_date}）")
    print(f"✅ 去重LLM日志：{dedup_log_path}")
    print(f"✅ 原始LLM返回目录：{RAW_DIR}/")
    print(f"✅ 最终写入：{DEDUPED_TABLE}（dedup_run_date={run_date}）")
    if EXPORT_FINAL_JSON:
        print(f"✅ 最终导出：{final_json_path}")


if __name__ == "__main__":
    main()