# -*- coding: utf-8 -*-
"""
光明网党建要闻 → 入库 news.db（站内去重）+ 自动写 article_category（可配置分类）
"""

from __future__ import annotations

import hashlib
import re
import time
from urllib.parse import urljoin

import requests
import sqlite3
from bs4 import BeautifulSoup
from readability import Document

DB_PATH = "news.db"

SOURCE = "gmw"
BASE = "https://dangjian.gmw.cn/"
LIST_URL = urljoin(BASE, "node_11941.htm")

HEADERS = {"User-Agent": "Mozilla/5.0 (DangjianCrawler/2.0)"}

# ====== 你只需要改这里：该爬虫抓到的分类 ======
CRAWLER_CATEGORIES = ["党建"]  # 例如 ["时政"] / ["党建","时政"]
# ===========================================

MAX_ITEMS = 30
REQUEST_DELAY = 0.6


# ----------------------------
# DB helpers
# ----------------------------
def get_category_ids(conn: sqlite3.Connection, names: list[str]) -> list[int]:
    ids = []
    for name in names:
        cur = conn.execute("SELECT id FROM categories WHERE name=? LIMIT 1", (name,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"分类不存在，请先在 categories 表插入：{name}")
        ids.append(int(row[0]))
    return ids


def link_article_categories(conn: sqlite3.Connection, article_id: int, category_ids: list[int]):
    for cid in category_ids:
        conn.execute(
            "INSERT OR IGNORE INTO article_category (article_id, category_id) VALUES (?, ?)",
            (article_id, cid),
        )


def exists_source_url(conn: sqlite3.Connection, source: str, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM articles WHERE source=? AND url=? LIMIT 1", (source, url))
    return cur.fetchone() is not None

def insert_article(conn: sqlite3.Connection, row: dict) -> int:
    """
    插入并返回 article_id
    """
    conn.execute(
        """
        INSERT INTO articles
        (source, url, title, pub_time, site_name, author,
         content_text, content_html, title_fp, content_fp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["source"],
            row["url"],
            row["title"],
            row.get("pub_time", ""),
            row.get("site_name", ""),
            row.get("author", ""),
            row["content_text"],
            row.get("content_html", ""),
            row["title_fp"],
            row["content_fp"],
        ),
    )
    # 取刚插入的 id
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ----------------------------
# HTTP + Parse
# ----------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


def clean_text_keep_paragraphs(s: str) -> str:
    s = s.replace("\u3000", " ")
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def norm_title(title: str) -> str:
    t = title.strip()
    t = t.replace("｜", "|").replace("丨", "|")
    t = re.sub(r"\s+", "", t)
    return t


def norm_content(text: str) -> str:
    return re.sub(r"\s+", "", (text or "").strip())


def parse_list_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []

    for li in soup.select("ul.channel-newsGroup > li"):
        a = li.select_one("span.channel-newsTitle a")
        t = li.select_one("span.channel-newsTime")
        if not a or not t:
            continue

        title = a.get_text(strip=True)
        href = (a.get("href") or "").strip()
        pub_date = t.get_text(strip=True)

        if not title or not href:
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", pub_date):
            continue

        url = urljoin(page_url, href)
        items.append({"title": title, "url": url, "pub_date": pub_date})

    # 最新优先
    items.sort(key=lambda x: x["pub_date"], reverse=True)

    # 列表页内去重
    seen = set()
    uniq = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        uniq.append(it)

    return uniq[:MAX_ITEMS]


def extract_gmw_detail(detail_html: str) -> dict:
    soup = BeautifulSoup(detail_html, "lxml")

    title_node = soup.select_one("h1#articleID")
    pub_time_node = soup.select_one("span#articlePubTime")
    source_node = soup.select_one("a#articleSource")

    title = title_node.get_text(strip=True) if title_node else ""
    pub_time = pub_time_node.get_text(strip=True) if pub_time_node else ""
    site_name = source_node.get_text(strip=True) if source_node else ""

    author = ""
    body = soup.select_one("#ContentPh")

    if body and len(body.get_text(strip=True)) > 200:
        for bad in body.select("script, style, noscript, iframe, .liability, .m-zbTool"):
            bad.decompose()
        for f in body.find_all("font"):
            f.unwrap()

        paras = []
        for p in body.find_all("p"):
            txt = p.get_text(" ", strip=True).strip()
            if not txt:
                continue
            if txt.startswith("作者："):
                author = txt.replace("作者：", "").strip()
            if txt.startswith(("责编：", "责任编辑：", "编辑：")):
                continue
            paras.append(txt)

        content_text = clean_text_keep_paragraphs("\n\n".join(paras) if paras else body.get_text(" ", strip=True))
        return {
            "title": title,
            "pub_time": pub_time,
            "site_name": site_name,
            "author": author,
            "content_text": content_text,
            "content_html": str(body),
        }

    # readability 兜底
    doc = Document(detail_html)
    html = doc.summary(html_partial=True)
    tmp = BeautifulSoup(html, "lxml")
    for bad in tmp.select("script, style, noscript, iframe"):
        bad.decompose()

    content_text = clean_text_keep_paragraphs(tmp.get_text("\n", strip=True))
    return {
        "title": title,
        "pub_time": pub_time,
        "site_name": site_name,
        "author": author,
        "content_text": content_text,
        "content_html": str(tmp),
    }


def main():
    print(f"▶ 光明网列表：{LIST_URL}")
    list_html = fetch_html(LIST_URL)
    items = parse_list_page(list_html, LIST_URL)
    if not items:
        raise RuntimeError("未解析到列表条目")

    conn = sqlite3.connect(DB_PATH)

    # 先查好分类 id（提前失败更清晰）
    category_ids = get_category_ids(conn, CRAWLER_CATEGORIES)

    inserted = 0
    skipped = 0
    failed = 0

    for i, it in enumerate(items, 1):
        url = it["url"]

        if exists_source_url(conn, SOURCE, url):
            skipped += 1
            print(f"  [{i}/{len(items)}] 已存在跳过：{it['title']}")
            continue

        print(f"  [{i}/{len(items)}] 抓详情入库：{it['title']}")
        try:
            detail_html = fetch_html(url)
            d = extract_gmw_detail(detail_html)

            title = d["title"] or it["title"]
            pub_time = d.get("pub_time") or (it["pub_date"] + " 00:00:00")

            row = {
                "source": SOURCE,
                "url": url,
                "title": title,
                "pub_time": pub_time,
                "site_name": d.get("site_name") or "光明网",
                "author": d.get("author") or "",
                "content_text": d["content_text"],
                "content_html": d.get("content_html") or "",
            }
            row["title_fp"] = sha1(norm_title(row["title"]))
            row["content_fp"] = sha1(norm_content(row["content_text"])[:2000])

            # 1) 插入文章
            article_id = insert_article(conn, row)
            # 2) 绑定分类
            link_article_categories(conn, article_id, category_ids)
            conn.commit()

            inserted += 1
        except Exception as e:
            conn.rollback()
            failed += 1
            print(f"    ❗失败：{e}")

        time.sleep(REQUEST_DELAY)

    conn.close()
    print("\n✅ 光明网完成")
    print(f"   新入库：{inserted}")
    print(f"   跳过：{skipped}")
    print(f"   失败：{failed}")
    print(f"   分类：{CRAWLER_CATEGORIES}")


if __name__ == "__main__":
    main()
