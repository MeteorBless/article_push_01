# -*- coding: utf-8 -*-
"""
【爬虫实现】中新网 - 时政频道
特点：
1. 列表页日期只有 MM-DD HH:MM，需要根据当前时间推断年份
2. 详情页元数据多存储在 hidden input 中
"""
import re
from datetime import date
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from readability import Document

from .base import BaseSpider
from utils import clean_text


class ChinaNewsSpider(BaseSpider):
    name = "chinanews"  # 对应 config.py 中的键名
    start_url = "https://www.chinanews.com.cn/china.shtml"
    categories = ["时政"]

    def _infer_year_for_mmdd(self, mm: int, dd: int) -> int:
        """根据月日推断年份：如果解析出的日期比今天还晚，说明是去年的新闻"""
        today = date.today()
        if (mm, dd) > (today.month, today.day):
            return today.year - 1
        return today.year

    def _parse_list_time(self, s: str) -> str:
        """解析 '2-11 09:37' 为 'YYYY-MM-DD HH:MM:00'"""
        if not s: return ""
        m = re.search(r"(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})", s.strip())
        if not m: return ""

        mm, dd, hh, mi = map(int, m.groups())
        y = self._infer_year_for_mmdd(mm, dd)
        return f"{y:04d}-{mm:02d}-{dd:02d} {hh:02d}:{mi:02d}:00"

    def parse_list(self, response):
        soup = BeautifulSoup(response.text, "lxml")
        items = []

        for li in soup.select("div.content_list ul li"):
            # 过滤分隔线、无内容标记和图片流模块
            if li.get("id") == "konge" or "nocontent" in (li.get("class") or []):
                continue
            if "photolm" in (li.get("class") or []):
                continue

            a = li.select_one(".dd_bt a")
            if not a or not a.get("href"):
                continue

            time_node = li.select_one(".dd_time")
            time_str = time_node.get_text(strip=True) if time_node else ""

            items.append({
                "title": a.get_text(strip=True),
                "url": urljoin(self.start_url, a.get("href").strip()),
                "pub_date": self._parse_list_time(time_str)
            })

        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)
        return uniq

    def parse_detail(self, response, item_meta):
        soup = BeautifulSoup(response.text, "lxml")

        # 1. 提取元数据 (优先使用页面中的 hidden input，准确度高)
        title = item_meta['title']
        nt = soup.select_one("#newstitle")
        if nt and nt.get("value"):
            title = nt["value"].strip()

        pub_time = item_meta.get('pub_date')
        nd = soup.select_one("#newsdate")
        ns = soup.select_one("#newstime")
        if nd and nd.get("value"):
            d_str = nd["value"].strip()
            t_str = ns["value"].strip() if (ns and ns.get("value")) else "00:00:00"
            pub_time = f"{d_str} {t_str}"

        site_name = "中新网"
        sb = soup.select_one("#source_baidu")
        if sb:
            site_name = sb.get_text(" ", strip=True).replace("来源：", "").strip()

        author = ""
        ed = soup.select_one("#editorname")
        if ed and ed.get("value"):
            author = ed["value"].strip()

        # 2. 提取正文
        body = soup.select_one("div.left_zw")
        content_text = ""
        content_html = ""

        def cleanup_dom(node):
            for bad in node.select("script, style, noscript, iframe, video, source, .videojsObj, .tupian_div"):
                bad.decompose()

        if body and len(body.get_text(strip=True)) > 80:
            cleanup_dom(body)
            # 移除尾部广告文案
            for p in body.find_all("p"):
                if p.get_text().strip().startswith("更多精彩内容请进入"):
                    p.decompose()

            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            # Readability 兜底
            doc = Document(response.text)
            summary_html = doc.summary(html_partial=True)
            tmp_soup = BeautifulSoup(summary_html, "lxml")
            cleanup_dom(tmp_soup)
            content_text = clean_text(tmp_soup.get_text())
            content_html = str(tmp_soup)

        return {
            "title": title,
            "pub_time": pub_time,
            "site_name": site_name,
            "author": author,
            "content_text": content_text,
            "content_html": content_html
        }