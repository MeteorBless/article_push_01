# -*- coding: utf-8 -*-
"""
共产党新闻网（人民网党建）→ 入库 news.db（站内去重）+ 自动写 article_category（可配置分类）
列表页：http://dangjian.people.com.cn/GB/394443/
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

SOURCE = "people"
LIST_URL = "http://dangjian.people.com.cn/GB/394443/"

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
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# ----------------------------
# HTTP + Parse
# ----------------------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "utf-8"
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


def normalize_pub_date_cn(s: str) -> str | None:
    if not s:
        return None
    m = re.search(r"(\d{4})年(\d{2})月(\d{2})日", s)
    if not m:
        return None
    y, mo, d = m.group(1), m.group(2), m.group(3)
    return f"{y}-{mo}-{d}"


def extract_source_and_date(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    pub_date = None
    source_name = None

    newstime = soup.select_one("#newstime")
    if newstime:
        pub_date = normalize_pub_date_cn(newstime.get_text(strip=True))

    sou = soup.select_one("p.sou")
    if sou:
        txt = sou.get_text(" ", strip=True)
        m = re.search(r"来源：\s*(.+)$", txt)
        if m:
            source_name = m.group(1).strip()

    return source_name, pub_date


def parse_list_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []

    # 列表结构：.p2j_con02 .fl ul li -> a + i(日期)
    for li in soup.select(".p2j_con02 .fl ul li"):
        a = li.find("a")
        if not a or not a.get("href"):
            continue
        title = a.get_text(strip=True)
        url = urljoin(page_url, a["href"].strip())

        i_tag = li.find("i")
        pub_date = normalize_pub_date_cn(i_tag.get_text(strip=True) if i_tag else "")

        items.append({"title": title, "url": url, "pub_date": pub_date})

    # 列表页内去重
    seen = set()
    uniq = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        uniq.append(it)

    return uniq[:MAX_ITEMS]


def extract_people_detail(detail_html: str) -> dict:
    soup = BeautifulSoup(detail_html, "lxml")

    h1 = soup.select_one(".text_c h1") or soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""

    source_name, pub_date = extract_source_and_date(soup)

    body = soup.select_one(".show_text")
    if body and len(body.get_text(strip=True)) > 100:
        for bad in body.select("script, style, noscript, iframe"):
            bad.decompose()

        paras = []
        for p in body.find_all("p"):
            txt = p.get_text(" ", strip=True).strip()
            if not txt:
                continue
            paras.append(txt)

        content_text = clean_text_keep_paragraphs("\n\n".join(paras) if paras else body.get_text(" ", strip=True))
        return {
            "title": title,
            "pub_date": pub_date,
            "source_name": source_name,
            "content_text": content_text,
            "content_html": str(body),
        }

    # 兜底 readability
    doc = Document(detail_html)
    html = doc.summary(html_partial=True)
    tmp = BeautifulSoup(html, "lxml")
    for bad in tmp.select("script, style, noscript, iframe"):
        bad.decompose()

    content_text = clean_text_keep_paragraphs(tmp.get_text("\n", strip=True))
    return {
        "title": title,
        "pub_date": pub_date,
        "source_name": source_name,
        "content_text": content_text,
        "content_html": str(tmp),
    }


def main():
    print(f"▶ 人民网党建列表：{LIST_URL}")
    list_html = fetch_html(LIST_URL)
    items = parse_list_page(list_html, LIST_URL)
    if not items:
        raise RuntimeError("未解析到列表条目")

    conn = sqlite3.connect(DB_PATH)

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
            d = extract_people_detail(detail_html)

            title = d["title"] or it["title"]
            # 这里先只保留日期；如果你想保留到分钟，我也可以再给你改一版解析 HH:MM
            pub_time = (d.get("pub_date") or it.get("pub_date") or "") + " 00:00:00"

            row = {
                "source": SOURCE,
                "url": url,
                "title": title,
                "pub_time": pub_time.strip(),
                "site_name": d.get("source_name") or "人民网",
                "author": "",
                "content_text": d["content_text"],
                "content_html": d.get("content_html") or "",
            }
            row["title_fp"] = sha1(norm_title(row["title"]))
            row["content_fp"] = sha1(norm_content(row["content_text"])[:2000])

            article_id = insert_article(conn, row)
            link_article_categories(conn, article_id, category_ids)
            conn.commit()

            inserted += 1
        except Exception as e:
            conn.rollback()
            failed += 1
            print(f"    ❗失败：{e}")

        time.sleep(REQUEST_DELAY)

    conn.close()
    print("\n✅ 人民网完成")
    print(f"   新入库：{inserted}")
    print(f"   跳过：{skipped}")
    print(f"   失败：{failed}")
    print(f"   分类：{CRAWLER_CATEGORIES}")


if __name__ == "__main__":
    main()
