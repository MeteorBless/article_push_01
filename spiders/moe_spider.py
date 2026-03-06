# -*- coding: utf-8 -*-
"""
【爬虫实现】中华人民共和国教育部 - 教育部简报
列表页：http://www.moe.gov.cn/jyb_sjzl/s3165/
特点：
1. 标准的 TRS WCM 静态分页机制：第一页为 index.html，第二页为 index_1.html，以此类推。
2. 列表存放在 ul#list 中。
3. 详情页非常规范，优先从 <meta> 标签中精确提取时间、来源、作者和标题，正文存在于 .TRS_Editor 中。
"""
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from readability import Document
from .base import BaseSpider
from utils import clean_text


class MoeBriefingSpider(BaseSpider):
    name = "moe_briefing"  # 对应 config.py 中的键名
    start_url = "http://www.moe.gov.cn/jyb_sjzl/s3165/"
    categories = ["时政"]  # 默认分类，教育部简报可归为时政或教育
    MAX_PAGES = 1  # 限制最大抓取页数

    def parse_list(self, response):
        """
        列表页解析：循环处理静态分页
        """
        self.logger.info("开启教育部简报静态翻页抓取...")
        items = []

        # TRS 系统的静态翻页规律：
        # 第1页: index.html
        # 第2页: index_1.html
        # 第3页: index_2.html
        for page in range(self.MAX_PAGES):
            if page == 0:
                page_url = urljoin(self.start_url, "index.html")
            else:
                # 兼容处理：虽然源码JS中拼接是 index_1.html，为防止个别偏差这里统一按规律构造
                page_url = urljoin(self.start_url, f"index_{page}.html")

            self.logger.info(f"正在抓取第 {page + 1} 页: {page_url}")

            resp = self.request(page_url)
            if not resp:
                self.logger.warning(f"第 {page + 1} 页请求失败或已到底，停止翻页。")
                break

            # 强制指定编码
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, "lxml")

            # 定位列表容器
            ul_list = soup.select_one("ul#list")
            if not ul_list:
                self.logger.info("当前页未匹配到文章列表，翻页结束。")
                break

            page_items_count = 0
            for li in ul_list.find_all("li"):
                a_tag = li.find("a")
                span_tag = li.find("span")

                if not a_tag or not a_tag.get("href"):
                    continue

                title = a_tag.get_text(strip=True)
                url = urljoin(page_url, a_tag.get("href").strip())

                # 提取日期 (YYYY-MM-DD)
                pub_date = span_tag.get_text(strip=True) if span_tag else ""

                items.append({
                    "title": title,
                    "url": url,
                    "pub_date": pub_date
                })
                page_items_count += 1

            if page_items_count == 0:
                break

            time.sleep(1)  # 防封延时

        # 列表页去重
        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)

        self.logger.info(f"静态翻页抓取完毕，共获取到 {len(uniq)} 条不重复文章。")
        return uniq

    def parse_detail(self, response, item_meta):
        """
        解析教育部详情页内容
        核心优势：充分利用政务网站规范的 <meta> 标签提取元数据
        """
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "lxml")

        # 1. 标题提取 (优先从 meta 提取，其次取 h1)
        title = ""
        meta_title = soup.find("meta", {"name": "ArticleTitle"})
        if meta_title and meta_title.get("content"):
            title = meta_title["content"].strip()
        else:
            title_node = soup.select_one(".moe-detail-box h1") or soup.find("h1")
            title = title_node.get_text(strip=True) if title_node else item_meta['title']

        # 2. 时间提取 (优先从 meta 提取精确到分的时间)
        pub_time = ""
        meta_date = soup.find("meta", {"name": "PubDate"})
        if meta_date and meta_date.get("content"):
            t_str = meta_date["content"].strip()
            # 如果是 '2026-02-28 13:48'，补齐秒数
            if len(t_str) == 16:
                pub_time = t_str + ":00"
            else:
                pub_time = t_str

        # 如果 meta 失败，使用列表页的时间兜底
        if not pub_time and item_meta.get('pub_date'):
            pub_time = item_meta['pub_date'] + " 00:00:00"

        # 3. 来源提取 (优先从 meta 提取)
        source_name = "教育部"
        meta_source = soup.find("meta", {"name": "ContentSource"})
        if meta_source and meta_source.get("content"):
            source_name = meta_source["content"].strip()

        # 4. 作者/编辑提取 (优先从 meta 提取)
        author = ""
        meta_author = soup.find("meta", {"name": "author"})
        if meta_author and meta_author.get("content"):
            author = meta_author["content"].strip()
        else:
            # 兜底寻找底部的责任编辑
            edit_node = soup.select_one("#detail-editor")
            if edit_node:
                txt = edit_node.get_text(strip=True)
                author = txt.replace("（", "").replace("）", "").replace("责任编辑：", "").strip()

        # 5. 正文提取
        body = soup.select_one(".TRS_Editor")
        content_text = ""
        content_html = ""

        if body:
            # 清除可能潜伏的脚本、样式等
            for bad in body.select("script, style, iframe"):
                bad.decompose()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            # 兜底：如果是非标准模板，使用 Readability
            doc = Document(response.text)
            summary = doc.summary(html_partial=True)
            content_html = summary
            content_text = clean_text(BeautifulSoup(summary, "lxml").get_text())

        return {
            "title": title,
            "pub_time": pub_time,
            "site_name": source_name,
            "author": author,
            "content_text": content_text,
            "content_html": content_html
        }