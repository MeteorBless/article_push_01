# -*- coding: utf-8 -*-
"""
【爬虫实现】光明网 - 党建频道
"""
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .base import BaseSpider
from utils import clean_text


class GuangmingSpider(BaseSpider):
    name = "gmw"  # 对应 config.py 中的键名
    start_url = "https://dangjian.gmw.cn/node_11941.htm"
    categories = ["党建"]

    def parse_list(self, response):
        """解析光明网列表结构：ul.channel-newsGroup > li"""
        soup = BeautifulSoup(response.text, "lxml")
        items = []
        for li in soup.select("ul.channel-newsGroup > li"):
            a = li.select_one("span.channel-newsTitle a")
            t = li.select_one("span.channel-newsTime")
            if a and t:
                pub_date = t.get_text(strip=True)
                # 简单过滤日期长度，确保是有效的日期字符串
                if len(pub_date) >= 10:
                    items.append({
                        "title": a.get_text(strip=True),
                        "url": urljoin(self.start_url, a.get("href")),
                        "pub_date": pub_date
                    })

        # 列表页内去重
        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)
        return uniq

    def parse_detail(self, response, item_meta):
        """解析光明网详情页"""
        soup = BeautifulSoup(response.text, "lxml")

        # 提取标题 (若详情页没有，回退使用列表页标题)
        title_node = soup.select_one("h1#articleID")
        title = title_node.get_text(strip=True) if title_node else item_meta['title']

        # 提取正文，去除无关脚本和样式
        body = soup.select_one("#ContentPh")
        if body:
            for bad in body.select("script, style, noscript, iframe, .liability, .m-zbTool"):
                bad.decompose()
            for f in body.find_all("font"):
                f.unwrap()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            content_text = ""
            content_html = ""

        return {
            "title": title,
            "pub_time": item_meta['pub_date'] + " 00:00:00",
            "site_name": "光明网",
            "author": "",
            "content_text": content_text,
            "content_html": content_html
        }