# -*- coding: utf-8 -*-
"""
daily_embedding_qwen.py

功能：
1) 从 news.db 读取“昨天”的文章
2) 使用 Qwen/Qwen3-Embedding-4B 生成 embedding
3) embedding 入库（article_embeddings）
4) 生成详细日志（可审计）

依赖：
pip install requests
"""

import os
import json
import time
import sqlite3
import datetime
import requests
import re
from typing import List, Dict

# ================== 配置 ==================
DB_PATH = "news.db"

# SiliconFlow / OpenAI 兼容 Embeddings API
EMBEDDING_URL = "https://api.siliconflow.cn/v1/embeddings"
EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-4B"

# 🔥 已按你要求：直接写死 API Key
API_KEY = "sk-eacrtalelzogpnvrgsreyjlygfugnrlomhpmbpkytxquyyia"

# 文本策略
CONTENT_PREVIEW_LEN = 1500     # 用多少正文参与 embedding
SLEEP_SECONDS = 0.6            # 防止请求过快

# 时间：每天跑前一天
TODAY = datetime.date.today()
TARGET_DATE = TODAY - datetime.timedelta(days=1)
# 如需手动指定日期，取消下面注释
# TARGET_DATE = datetime.date(2026, 1, 28)

# 日志
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
# =========================================


def now_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"^作者[:：].{0,60}?(\s|，|。)", "", text)
    return text.strip()


def strip_html(html: str) -> str:
    if not html:
        return ""
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    html = re.sub(r"(?is)<.*?>", " ", html)
    return clean_text(html)


def fetch_articles(conn: sqlite3.Connection, target_date: datetime.date) -> List[Dict]:
    day = target_date.strftime("%Y-%m-%d")
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT id, title, source, pub_time, content_text, content_html
        FROM articles
        WHERE pub_time LIKE ?
        ORDER BY pub_time DESC
        """,
        (f"{day}%",)
    ).fetchall()

    if not rows:
        rows = cur.execute(
            """
            SELECT id, title, source, pub_time, content_text, content_html
            FROM articles
            WHERE fetched_at >= ? AND fetched_at <= ?
            ORDER BY fetched_at DESC
            """,
            (f"{day} 00:00:00", f"{day} 23:59:59")
        ).fetchall()

    items = []
    for r in rows:
        text = clean_text(r[4] or "")
        if not text:
            text = strip_html(r[5] or "")
        items.append({
            "id": r[0],
            "title": r[1] or "",
            "source": r[2] or "",
            "pub_time": r[3] or "",
            "content": text
        })
    return items


def already_embedded(conn: sqlite3.Connection, article_id: int) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT 1 FROM article_embeddings WHERE article_id=? AND model=?",
        (article_id, EMBEDDING_MODEL)
    ).fetchone()
    return row is not None


def call_embedding(text: str) -> List[float]:
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": EMBEDDING_MODEL,
        "input": text
    }

    resp = requests.post(
        EMBEDDING_URL,
        headers=headers,
        json=payload,
        timeout=60
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Embedding API 失败: HTTP {resp.status_code}\n{resp.text}"
        )

    data = resp.json()
    return data["data"][0]["embedding"]


def main():
    run_day = TARGET_DATE.strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"embedding_log_{run_day}.txt")

    conn = sqlite3.connect(DB_PATH)
    articles = fetch_articles(conn, TARGET_DATE)

    if not articles:
        raise RuntimeError(f"{run_day} 没有文章")

    with open(log_path, "w", encoding="utf-8") as log_f:
        log_f.write(f"=== Embedding 日期：{run_day} 文章数：{len(articles)} ===\n")
        log_f.write(f"MODEL={EMBEDDING_MODEL}\n\n")

        inserted = 0
        skipped = 0

        for idx, a in enumerate(articles, 1):
            aid = a["id"]

            if already_embedded(conn, aid):
                skipped += 1
                continue

            text = (
                f"{a['title']}\n"
                f"{a['content'][:CONTENT_PREVIEW_LEN]}"
            ).strip()

            log_f.write(
                f"[{now_ts()}] ({idx}/{len(articles)}) "
                f"embedding article_id={aid} pub_time={a['pub_time']}\n"
            )

            vec = call_embedding(text)
            dim = len(vec)

            conn.execute(
                """
                INSERT OR REPLACE INTO article_embeddings
                (article_id, model, embedding, dim)
                VALUES (?, ?, ?, ?)
                """,
                (aid, EMBEDDING_MODEL, json.dumps(vec), dim)
            )
            conn.commit()

            log_f.write(f"  -> dim={dim} 写入完成\n")
            inserted += 1

            time.sleep(SLEEP_SECONDS)

        log_f.write("\n=== 统计 ===\n")
        log_f.write(f"写入 embedding：{inserted}\n")
        log_f.write(f"跳过已存在：{skipped}\n")

    conn.close()

    print(f"✅ Embedding 完成：{run_day}")
    print(f"✅ 日志：{log_path}")


if __name__ == "__main__":
    main()
