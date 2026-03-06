# -*- coding: utf-8 -*-
"""
中国新闻网（时政）→ 入库 news.db（站内去重）+ 自动写 article_category（可配置分类）
列表页：https://www.chinanews.com.cn/china.shtml
"""

from __future__ import annotations

import hashlib
import re
import time
import sqlite3
from datetime import datetime, date
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from readability import Document

DB_PATH = "news.db"

SOURCE = "chinanews"
LIST_URL = "https://www.chinanews.com.cn/china.shtml"
BASE_URL = "https://www.chinanews.com.cn"

HEADERS = {"User-Agent": "Mozilla/5.0 (ChinaNewsCrawler/1.0)"}

# ====== 你只需要改这里：该爬虫抓到的分类 ======
CRAWLER_CATEGORIES = ["时政"]  # 例如 ["时政"] / ["国内","时政"]
# ===========================================

MAX_ITEMS = 30
REQUEST_DELAY = 0.6


# ----------------------------
# DB helpers（与 rm_tosql.py 保持一致）
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


def infer_year_for_mmdd(mm: int, dd: int) -> int:
    """列表页时间通常是 M-D HH:MM，不带年；用今天推断年份：若 M-D 比今天“更晚”，认为是去年。"""
    today = date.today()
    if (mm, dd) > (today.month, today.day):
        return today.year - 1
    return today.year


def parse_list_time(s: str) -> str | None:
    """
    输入形如：'2-11 09:37' -> 'YYYY-MM-DD HH:MM:00'
    """
    if not s:
        return None
    m = re.search(r"(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", s.strip())
    if not m:
        return None
    mm, dd, hh, mi = map(int, m.groups())
    y = infer_year_for_mmdd(mm, dd)
    return f"{y:04d}-{mm:02d}-{dd:02d} {hh:02d}:{mi:02d}:00"


def parse_list_page(html: str, page_url: str) -> list[dict]:
    """
    列表结构来自：div.content_list ul li 里有 dd_bt a / dd_time / dd_lm
    """
    soup = BeautifulSoup(html, "lxml")
    items: list[dict] = []

    for li in soup.select("div.content_list ul li"):
        # 页面里有 <li id=konge> 分隔符
        if li.get("id") == "konge":
            continue
        cls = li.get("class") or []
        if "nocontent" in cls:
            continue
        # 图片流/特殊模块（如 photolm）结构不同；先跳过，后续需要我可以再补解析
        if "photolm" in cls:
            continue

        a = li.select_one(".dd_bt a")
        if not a or not a.get("href"):
            continue

        title = a.get_text(strip=True)
        url = urljoin(page_url, a["href"].strip())
        pub_time = parse_list_time((li.select_one(".dd_time") or {}).get_text(strip=True) if li.select_one(".dd_time") else "")

        lm = li.select_one(".dd_lm")
        lm_text = lm.get_text(strip=True) if lm else ""

        items.append({"title": title, "url": url, "pub_time": pub_time, "lm": lm_text})

    # 列表页内去重
    seen = set()
    uniq = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        uniq.append(it)

    return uniq[:MAX_ITEMS]


def extract_chinanews_detail(detail_html: str) -> dict:
    soup = BeautifulSoup(detail_html, "lxml")

    # 1) 标题：优先 hidden input#newstitle
    title = ""
    nt = soup.select_one("#newstitle")
    if nt and nt.get("value"):
        title = nt["value"].strip()
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

    # 2) 发布时间：hidden input newsdate + newstime
    pub_time = ""
    nd = soup.select_one("#newsdate")
    ns = soup.select_one("#newstime")
    if nd and nd.get("value"):
        pub_time = nd["value"].strip()
        if ns and ns.get("value"):
            pub_time = f"{pub_time} {ns['value'].strip()}"
        else:
            pub_time = f"{pub_time} 00:00:00"

    # 3) 来源：span#source_baidu 里通常有 “来源：<a>新华社</a>”
    source_name = ""
    sb = soup.select_one("#source_baidu")
    if sb:
        a = sb.find("a")
        if a:
            source_name = a.get_text(strip=True)
        else:
            source_name = sb.get_text(" ", strip=True).replace("来源：", "").strip()

    # 4) 作者/责任编辑：用 editorname（hidden）
    author = ""
    ed = soup.select_one("#editorname")
    if ed and ed.get("value"):
        author = ed["value"].strip()

    # 5) 正文：优先 div.left_zw（样例页存在），否则 readability 兜底
    body = soup.select_one("div.left_zw")
    content_html = ""
    content_text = ""

    def cleanup(node):
        for bad in node.select("script, style, noscript, iframe, video, source"):
            bad.decompose()
        # 常见视频容器
        for bad in node.select(".videojsObj, .tupian_div"):
            bad.decompose()

    if body and len(body.get_text(strip=True)) > 80:
        cleanup(body)
        paras = []
        for p in body.find_all("p"):
            txt = p.get_text(" ", strip=True).strip()
            if not txt:
                continue
            # 简单过滤一些尾部/按钮文案（可按需要再加规则）
            if txt.startswith("更多精彩内容请进入"):
                continue
            paras.append(txt)

        content_text = clean_text_keep_paragraphs("\n\n".join(paras) if paras else body.get_text("\n", strip=True))
        content_html = str(body)
    else:
        doc = Document(detail_html)
        html = doc.summary(html_partial=True)
        tmp = BeautifulSoup(html, "lxml")
        cleanup(tmp)
        content_text = clean_text_keep_paragraphs(tmp.get_text("\n", strip=True))
        content_html = str(tmp)

    return {
        "title": title,
        "pub_time": pub_time,
        "source_name": source_name,
        "author": author,
        "content_text": content_text,
        "content_html": content_html,
    }


def main():
    print(f"▶ 中国新闻网时政列表：{LIST_URL}")
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
            d = extract_chinanews_detail(detail_html)

            title = d["title"] or it["title"]
            pub_time = (d.get("pub_time") or it.get("pub_time") or "").strip()

            row = {
                "source": SOURCE,
                "url": url,
                "title": title,
                "pub_time": pub_time,
                # 与 rm_tosql.py 一样：site_name 存“来源/机构”，没有就写“中新网”
                "site_name": d.get("source_name") or "中新网",
                "author": d.get("author") or "",
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
    print("\n✅ 中新网完成")
    print(f"   新入库：{inserted}")
    print(f"   跳过：{skipped}")
    print(f"   失败：{failed}")
    print(f"   分类：{CRAWLER_CATEGORIES}")


if __name__ == "__main__":
    main()
