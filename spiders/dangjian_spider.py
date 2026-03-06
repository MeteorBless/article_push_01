# -*- coding: utf-8 -*-
"""
【爬虫实现】党建网 - 理论强党
列表页：http://www.dangjian.cn/llqd/list_50754_1.html
特点：
1. 列表页数据通过 JS 接口动态加载，返回的是包含 JS 变量声明的字符串。
2. 提取变量 `MI4_PAGE_ARTICLE` 的值即可获得干净的 JSON 数组。
3. 接口自带丰富的元数据（作者、来源、发布时间等），可直接传递给详情页使用。
"""
import re
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from readability import Document
from .base import BaseSpider
from utils import clean_text, get_random_header


class DangjianTheorySpider(BaseSpider):
    name = "dangjian_theory"
    start_url = "http://www.dangjian.cn/llqd/list_50754_1.html"
    categories = ["党建"]

    # 真正的 JS API 数据接口
    API_BASE = "http://www.dangjian.cn/mi4-rest-api/pageArticles.js"
    MAX_PAGES = 1  # 限制抓取页数

    def parse_list(self, response):
        """
        【极速解析】利用抓包得到的 JS 接口直接获取数据，无视 HTML 渲染
        """
        self.logger.info("开启接口直连模式，解析党建网 JS 数据...")
        items = []
        page = 1

        # 伪装请求头
        headers = get_random_header()
        headers["Referer"] = self.start_url
        headers["Accept"] = "*/*"

        while page <= self.MAX_PAGES:
            # 构造请求参数
            params = {
                "wbId": "91",
                "subjectId": "50754",
                "page": str(page),
                "limit": "25",
                "noChild": "1"
            }

            self.logger.info(f"正在抓取第 {page} 页 API 数据...")

            # 发送请求
            api_resp = self.session.get(
                self.API_BASE,
                params=params,
                headers=headers,
                timeout=10,
                verify=False
            )

            if not api_resp or api_resp.status_code != 200:
                self.logger.warning(f"第 {page} 页请求失败，停止翻页。")
                break

            # 强制指定编码（避免中文乱码）
            api_resp.encoding = 'utf-8'
            text = api_resp.text.strip()

            # 使用正则提取 var MI4_PAGE_ARTICLE = [{...}] 中的数组部分
            m = re.search(r'MI4_PAGE_ARTICLE\s*=\s*(\[.*\])', text, re.DOTALL)
            if not m:
                self.logger.warning("未匹配到 MI4_PAGE_ARTICLE 变量数据，停止翻页。")
                break

            json_str = m.group(1)

            try:
                data_list = json.loads(json_str)

                if not data_list:
                    self.logger.info("当前页已无数据，翻页结束。")
                    break

                for item in data_list:
                    # 优先使用外链，没有外链则使用内部 URL
                    raw_url = item.get("external_link") or item.get("url", "")
                    title = item.get("title", "")

                    if not raw_url or not title:
                        continue

                    url = urljoin(self.start_url, raw_url)

                    # 提取发布时间 (接口给的是 2026-03-03 16:50，补充秒数)
                    pub_date = item.get("pub_date", "").strip()
                    if len(pub_date) == 16:  # YYYY-MM-DD HH:MM
                        pub_date += ":00"

                    # 提取接口中自带的来源和作者
                    source_name = item.get("miOrigin", "")
                    author = item.get("miRespAuthor") or item.get("miAuthor") or ""

                    items.append({
                        "title": title,
                        "url": url,
                        "pub_date": pub_date,
                        "source_name": source_name,
                        "author": author
                    })

                page += 1
                time.sleep(1)  # 防封延时

            except json.JSONDecodeError as e:
                self.logger.error(f"JSON 解析失败: {e}")
                break

        # 列表去重
        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)

        self.logger.info(f"API 抓取完毕，共获取到 {len(uniq)} 条不重复文章。")
        return uniq

    def parse_detail(self, response, item_meta):
        """
        解析详情页内容 (提取 HTML 标签)
        """
        soup = BeautifulSoup(response.text, "lxml")

        # 1. 标题提取
        title = ""
        title_node = soup.select_one("#title_tex p") or soup.select_one(".context-tit p")
        if title_node:
            title = title_node.get_text(strip=True)
        else:
            title = item_meta['title']

        # 2. 时间提取 (优先取页面，否则用列表页元数据)
        pub_time = ""
        time_node = soup.select_one("#time_tex")
        if time_node:
            pub_time = time_node.get_text(strip=True).replace("发表时间：", "").strip()

        if not pub_time and item_meta.get('pub_date'):
            pub_time = item_meta['pub_date']

        # 3. 来源提取 (优先取页面，否则用列表页元数据)
        source_name = item_meta.get('source_name', "党建网")
        source_node = soup.select_one("#time_ly")
        if source_node:
            source_name = source_node.get_text(strip=True).replace("来源：", "").strip()

        # 4. 作者/编辑提取
        author = item_meta.get('author', "")
        author_node = soup.select_one(".bj span")
        if author_node:
            author = author_node.get_text(strip=True)

        # 5. 正文提取 (优先 .TRS_Editor，其次 #tex)
        body = soup.select_one(".TRS_Editor") or soup.select_one("#tex")
        content_text = ""
        content_html = ""

        if body:
            # 清理视频占位符和无关脚本
            for bad in body.select("script, style, iframe, .video-sj"):
                bad.decompose()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
            # 兜底
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