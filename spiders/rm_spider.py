# -*- coding: utf-8 -*-
"""
【爬虫实现】人民网 - 党建频道
特点：需要处理中文日期格式 (YYYY年MM月DD日)
"""
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from readability import Document
from .base import BaseSpider
from utils import clean_text
import re


class RenminSpider(BaseSpider):
    name = "people"  # 对应 config.py 中的键名
    start_url = "http://dangjian.people.com.cn/GB/394443/"
    categories = ["党建"]

    def _normalize_date(self, s):
        """将 'YYYY年MM月DD日' 转换为 'YYYY-MM-DD'"""
        if not s: return None
        m = re.search(r"(\d{4})年(\d{2})月(\d{2})日", s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return None

    def parse_list(self, response):
        soup = BeautifulSoup(response.text, "lxml")
        items = []
        # CSS选择器：.p2j_con02 .fl ul li -> a + i(日期)
        for li in soup.select(".p2j_con02 .fl ul li"):
            a = li.find("a")
            if not a or not a.get("href"):
                continue

            i_tag = li.find("i")
            pub_date = self._normalize_date(i_tag.get_text(strip=True) if i_tag else "")

            items.append({
                "title": a.get_text(strip=True),
                "url": urljoin(self.start_url, a["href"].strip()),
                "pub_date": pub_date
            })

        # 列表页去重
        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)
        return uniq

    def parse_detail(self, response, item_meta):
        soup = BeautifulSoup(response.text, "lxml")

        h1 = soup.select_one(".text_c h1") or soup.find("h1")
        title = h1.get_text(strip=True) if h1 else item_meta['title']

        source_name = "人民网"
        sou = soup.select_one("p.sou")
        if sou:
            txt = sou.get_text(" ", strip=True)
            m = re.search(r"来源：\s*(.+)$", txt)
            if m:
                source_name = m.group(1).strip()

        # 尝试提取正文
        body = soup.select_one(".show_text")
        if body and len(body.get_text(strip=True)) > 50:
            for bad in body.select("script, style, noscript, iframe"):
                bad.decompose()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            # 如果常规提取失败，使用 readability 库兜底
            doc = Document(response.text)
            summary = doc.summary(html_partial=True)
            tmp = BeautifulSoup(summary, "lxml")
            content_text = clean_text(tmp.get_text())
            content_html = str(tmp)

        pub_time = item_meta.get('pub_date') or ""
        if pub_time:
            pub_time += " 00:00:00"

        return {
            "title": title,
            "pub_time": pub_time,
            "site_name": source_name,
            "author": "",
            "content_text": content_text,
            "content_html": content_html
        }