# -*- coding: utf-8 -*-
"""
【爬虫实现】人民网 - 科技频道
列表页：http://scitech.people.com.cn/
分页格式：http://scitech.people.com.cn/index1.html, index2.html ...
特点：
1. 列表页完全静态，通过循环请求 index{page}.html 即可实现翻页。
2. 列表存放在 .ej_list_box ul.list_16 中。
3. 详情页具有规范的 id="newstime" (时间) 和 id="rm_txt_zw" (正文)。
"""
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from readability import Document
from .base import BaseSpider
from utils import clean_text


class PeopleSciTechSpider(BaseSpider):
    name = "people_scitech"  # 对应 config.py 中的键名
    start_url = "http://scitech.people.com.cn/index1.html"
    categories = ["科技"]
    MAX_PAGES = 1  # 限制最大抓取页数

    def parse_list(self, response):
        """
        列表页解析：忽略传入的单页 response，直接在内部循环处理分页
        """
        self.logger.info("开启人民网科技频道静态翻页抓取...")
        items = []

        for page in range(1, self.MAX_PAGES + 1):
            # 构造分页 URL
            page_url = f"http://scitech.people.com.cn/index{page}.html"
            self.logger.info(f"正在抓取第 {page} 页: {page_url}")

            # 使用基类的 request 发送请求 (自带防封、UA 和重试)
            resp = self.request(page_url)
            if not resp:
                self.logger.warning(f"第 {page} 页请求失败，停止翻页。")
                break

            # 强制使用 utf-8 解码，防止中文乱码
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, "lxml")

            # 定位列表容器
            ul_lists = soup.select(".ej_list_box ul.list_16")
            if not ul_lists:
                self.logger.info("当前页未匹配到文章列表，翻页结束。")
                break

            page_items_count = 0
            for ul in ul_lists:
                for li in ul.find_all("li"):
                    a_tag = li.find("a")
                    em_tag = li.find("em")

                    if not a_tag or not a_tag.get("href"):
                        continue

                    title = a_tag.get_text(strip=True)
                    url = urljoin(page_url, a_tag.get("href").strip())

                    # 提取日期 (YYYY-MM-DD)
                    pub_date = em_tag.get_text(strip=True) if em_tag else ""

                    items.append({
                        "title": title,
                        "url": url,
                        "pub_date": pub_date
                    })
                    page_items_count += 1

            if page_items_count == 0:
                break

            time.sleep(1)  # 礼貌性延时

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
        解析人民网详情页内容
        """
        # 强制 utf-8 编码
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "lxml")

        # 1. 标题提取 (优先 h1)
        title = ""
        title_node = soup.select_one("h1")
        if title_node:
            title = title_node.get_text(strip=True)
        else:
            title = item_meta['title']

        # 2. 时间提取
        pub_time = ""
        time_node = soup.select_one("#newstime")
        if time_node:
            t_str = time_node.get_text(strip=True)
            # 格式转换：2026年03月04日09:04 -> 2026-03-04 09:04:00
            t_str = t_str.replace("年", "-").replace("月", "-").replace("日", " ")
            if len(t_str.split()) == 2:
                pub_time = t_str + ":00"
            else:
                pub_time = t_str

        if not pub_time and item_meta.get('pub_date'):
            pub_time = item_meta['pub_date'] + " 00:00:00"

        # 3. 来源提取 (格式：来源：<a href="...">科技日报</a>)
        source_name = "人民网"
        src_tag = soup.find(string=re.compile("来源："))
        if src_tag and src_tag.parent:
            parent = src_tag.parent
            a_tag = parent.find("a")
            if a_tag:
                source_name = a_tag.get_text(strip=True)
            else:
                txt = parent.get_text(strip=True)
                m = re.search(r"来源：(.*?)(?=\s|$)", txt)
                if m:
                    source_name = m.group(1).replace("来源：", "").strip()

        # 4. 作者/编辑提取 (格式：(责编：杨曦、陈键))
        author = ""
        edit_node = soup.select_one(".edit")
        if edit_node:
            txt = edit_node.get_text(strip=True)
            txt = txt.replace("(", "").replace(")", "").replace("（", "").replace("）", "")
            txt = txt.replace("责编：", "").strip()
            author = txt

        # 5. 正文提取
        body = soup.select_one("#rm_txt_zw")
        content_text = ""
        content_html = ""

        if body:
            # 清除图片外框、分页符、中心对齐的无用表格等
            for bad in body.select(".box_pic, .zdfy, script, style, iframe, center"):
                bad.decompose()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            # 兜底：如果改版导致 id 变化，使用 Readability
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