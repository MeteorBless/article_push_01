# -*- coding: utf-8 -*-
"""
【爬虫实现】新华网专题 - 树立和践行正确政绩观学习教育 (最新播报)
列表页：https://www.news.cn/zt/slhjxzqzjg/zxbb.html
特点：
1. 列表容器为 ul.wz-list，并且 <li> 标签上自带 data-pubtime 属性。
2. 同样采用 datasource ID 生成的静态 JSON 存储全量数据。
3. 详情页与常规新华网新闻结构一致。
"""
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from readability import Document
from .base import BaseSpider
from utils import clean_text


class XinhuaZtSpider(BaseSpider):
    name = "xinhua_zt"  # 对应 config.py 中的键名
    start_url = "https://www.news.cn/zt/slhjxzqzjg/zxbb.html"
    categories = ["党建"]  # 专题性质明显，归类为党建

    def parse_list(self, response):
        """
        列表页解析：整合静态 HTML 提取与 JSON 全量提取
        """
        soup = BeautifulSoup(response.text, "lxml")
        items = []
        datasource_id = None

        self.logger.info("=== 第一步：从专题 HTML 页面获取默认的最新数据 ===")
        # 专题页列表容器是 ul.wz-list
        ul = soup.select_one("ul.wz-list")

        if ul:
            # 1. 尝试动态提取 data 属性里的 datasource ID
            if ul.get("data"):
                m = re.search(r"datasource:([a-zA-Z0-9]+)", ul["data"])
                if m:
                    datasource_id = m.group(1)

            # 2. 解析 HTML 里的 <li>
            for li in ul.find_all("li"):
                a_tag = li.find("a")
                if not a_tag or not a_tag.get("href"):
                    continue

                title = a_tag.get_text(strip=True)
                url = urljoin(self.start_url, a_tag.get("href").strip())

                # 专题页直接将完整时间写在了 <li> 的 data-pubtime 属性里
                pub_time = li.get("data-pubtime", "").strip()

                items.append({
                    "title": title,
                    "url": url,
                    "pub_date": pub_time
                })

        self.logger.info(f"HTML 页面获取完成，当前共有 {len(items)} 篇文章。")

        self.logger.info("=== 第二步：从全量 JSON 接口获取剩余更多数据 ===")
        # 如果未能在页面中动态提取到 ID，则使用你提供的目标 ID
        if not datasource_id:
            datasource_id = "e42e97b901a348acb75d913ba8954b72"
            self.logger.info(f"未动态提取到 datasource ID，使用默认 ID: {datasource_id}")

        # 拼接全量 JSON 的 URL
        # json_url 结果为: https://www.news.cn/zt/slhjxzqzjg/ds_e42e97b901a348acb75d913ba8954b72.json
        json_url = urljoin(self.start_url, f"ds_{datasource_id}.json")
        self.logger.info(f"请求 JSON 接口: {json_url}")

        json_resp = self.request(json_url)
        if json_resp:
            try:
                json_resp.encoding = 'utf-8'
                json_data = json_resp.json()

                # 数据存在 'datasource' 字段中
                ds_items = json_data.get('datasource', [])
                if not ds_items and isinstance(json_data, list):
                    ds_items = json_data

                new_count = 0
                for item in ds_items:
                    raw_title = item.get('showTitle') or item.get('title', '')
                    raw_url = item.get('publishUrl') or item.get('url', '')
                    pub_time = item.get('publishTime', '')

                    if not raw_url:
                        continue

                    link = urljoin(self.start_url, raw_url)
                    # 净化标题中可能混入的 HTML 标签
                    clean_title = BeautifulSoup(raw_title, "html.parser").get_text(strip=True)

                    items.append({
                        "title": clean_title,
                        "url": link,
                        "pub_date": pub_time
                    })
                    new_count += 1
                self.logger.info(f"JSON 接口获取完成，合并提取到 {new_count} 条数据。")

            except Exception as e:
                self.logger.error(f"获取或解析 JSON 数据失败: {e}")

        # --- 第三步：列表页网址去重 ---
        seen = set()
        uniq = []
        for it in items:
            if it['url'] not in seen:
                seen.add(it['url'])
                uniq.append(it)

        self.logger.info(f"去重后总计得到 {len(uniq)} 条文章链接，准备进入详情页抓取...")
        return uniq

    def parse_detail(self, response, item_meta):
        """
        解析详情页内容
        """
        soup = BeautifulSoup(response.text, "lxml")

        # 1. 标题提取
        title = ""
        title_node = soup.select_one(".head-line .title") or soup.find("h1")
        if title_node:
            title = title_node.get_text(strip=True)
        else:
            title = item_meta['title']

        # 2. 时间提取 (优先从详情页提取精确时间)
        pub_time = ""
        time_container = soup.select_one(".header-time")
        if time_container:
            try:
                year = time_container.select_one(".year").get_text(strip=True)
                day = time_container.select_one(".day").get_text(strip=True)
                time_str = time_container.select_one(".time").get_text(strip=True)

                day_fmt = day.replace("/", "-")
                if not day_fmt.startswith("-"):
                    day_fmt = "-" + day_fmt
                pub_time = f"{year}{day_fmt} {time_str}"
            except Exception:
                pass

        # 如果详情页没解析出来，使用列表页的时间兜底
        if not pub_time and item_meta.get('pub_date'):
            pub_time = item_meta['pub_date']
            # 如果仅仅是 YYYY-MM-DD，补上时分秒
            if len(pub_time) == 10:
                pub_time += " 00:00:00"

        # 3. 来源提取
        source_name = "新华网"
        source_node = soup.select_one(".source")
        if source_node:
            txt = source_node.get_text(strip=True)
            if "来源：" in txt:
                source_name = txt.replace("来源：", "").strip()

        # 4. 作者/编辑提取
        author = ""
        editor_node = soup.select_one(".editor")
        if editor_node:
            txt = editor_node.get_text(strip=True)
            txt = txt.replace("【", "").replace("】", "").replace("责任编辑", "").replace(":", "").replace("：", "")
            author = txt.strip()

        # 5. 正文提取 (优先 span#detailContent)
        body = soup.select_one("span#detailContent") or soup.select_one("#detail")
        content_text = ""
        content_html = ""

        if body:
            # 清理无关标签，特别是 #articleEdit 下的纠错信息
            for bad in body.select("script, style, iframe, .tiyi1, .editor, #articleEdit, .advise"):
                bad.decompose()
            content_text = clean_text(body.get_text())
            content_html = str(body)
        else:
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