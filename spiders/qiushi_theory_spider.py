# -*- coding: utf-8 -*-
"""
【爬虫实现】求是网 - 关键词检索
特点：
1. 通过 search.qstheory.cn 的 JSONP 接口按关键词分页检索
2. 兼容 JSON / HTML 片段两种返回结构
3. 按 source+url 判重后入库
"""
import json
import re
import time
import uuid
from datetime import datetime
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup

import config
from db_manager import DBManager
from utils import clean_text, sha1
from .base import BaseSpider


class QiushiTheorySpider(BaseSpider):
    name = "qstheory"
    start_url = "https://search.qstheory.cn/qiushi"

    # 与原始方案保持一致：按关键词抓取近一年数据
    keywords = ["安全"]  # , "高质量发展", "中国式现代化
    max_pages_per_keyword = 3
    api_url = "https://search.qstheory.cn/qiushi/moreNew?callback=jsonpCallback"
    base_url = "https://www.qstheory.cn"

    def parse_list(self, response):
        """该站点使用关键词检索接口，列表抓取在 run 中实现。"""
        return []

    def _extract_pub_time(self, soup):
        """提取发布时间，返回 YYYY-MM-DD HH:MM:SS。"""
        meta_candidates = [
            "meta[property='article:published_time']",
            "meta[name='pubtime']",
            "meta[name='publishdate']",
            "meta[name='publishDate']",
            "meta[name='PubDate']",
            "meta[itemprop='datePublished']",
        ]
        for selector in meta_candidates:
            node = soup.select_one(selector)
            if node and node.get("content"):
                normalized = self._normalize_pub_time(node.get("content"))
                if normalized:
                    return normalized

        text_candidates = [
            "div.info",
            "div.source",
            "div.pages-date",
            "span.date",
            "span.time",
            "p.time",
        ]
        for selector in text_candidates:
            node = soup.select_one(selector)
            if not node:
                continue
            normalized = self._normalize_pub_time(node.get_text(" ", strip=True))
            if normalized:
                return normalized

        full_text = soup.get_text(" ", strip=True)
        return self._normalize_pub_time(full_text)

    def _normalize_pub_time(self, raw):
        if not raw:
            return ""
        text = clean_text(raw)
        text = text.replace("年", "-").replace("月", "-").replace("日", " ")
        text = text.replace("/", "-")

        match = re.search(r"(\d{4}-\d{1,2}-\d{1,2})(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?", text)
        if not match:
            return ""

        date_part = match.group(1)
        time_part = match.group(2) or "00:00:00"

        if len(time_part) == 5:
            time_part += ":00"

        try:
            dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""

    def _extract_author(self, soup):
        """提取作者。"""
        meta_candidates = [
            "meta[name='author']",
            "meta[property='article:author']",
            "meta[name='Author']",
        ]
        for selector in meta_candidates:
            node = soup.select_one(selector)
            if node and node.get("content"):
                return clean_text(node.get("content"))

        text_candidates = [
            "span.author",
            "p.author",
            "div.author",
            "span.editor",
            "div.editor",
        ]
        for selector in text_candidates:
            node = soup.select_one(selector)
            if not node:
                continue
            txt = clean_text(node.get_text(" ", strip=True))
            txt = txt.replace("作者：", "").replace("作者:", "")
            txt = txt.replace("责任编辑：", "").replace("责任编辑:", "")
            if txt:
                return txt

        full_text = soup.get_text(" ", strip=True)
        m = re.search(r"(?:作者|责任编辑)[：:]\s*([^\s|｜/]{2,30})", full_text)
        return m.group(1).strip() if m else ""

    def parse_detail(self, response, item_meta):
        """解析详情页正文。"""
        soup = BeautifulSoup(response.text, "lxml")

        title = item_meta["title"]
        title_node = soup.find("h1") or soup.select_one(".title")
        if title_node and title_node.get_text(strip=True):
            title = title_node.get_text(strip=True)

        content_box = (
            soup.find("div", id="detailContent")
            or soup.find("div", class_="highlight")
            or soup.find("div", class_="text")
        )

        content_html = ""
        content_text = ""
        if content_box:
            paragraphs = [p.get_text(strip=True) for p in content_box.find_all("p")]
            if paragraphs:
                content_text = clean_text("\n".join([p for p in paragraphs if p]))
            else:
                content_text = clean_text(content_box.get_text("\n", strip=True))
            content_html = str(content_box)

        return {
            "title": title,
            "pub_time": self._extract_pub_time(soup),
            "site_name": "求是网",
            "author": self._extract_author(soup),
            "content_text": content_text,
            "content_html": content_html,
        }

    def _one_year_ago_date(self):
        now = datetime.now()
        return f"{now.year - 1}.{now.month}.{now.day}"

    def _extract_items_from_json(self, data, items):
        if isinstance(data, dict):
            url = data.get("url") or data.get("LinkUrl") or data.get("docpuburl") or data.get("docUrl")
            title = data.get("title") or data.get("LinkTitle") or data.get("IntroTitle") or data.get("doctitle")

            if url and isinstance(url, str):
                clean_title = BeautifulSoup(title or "", "html.parser").get_text(strip=True) or "未知标题"
                full_url = urljoin(self.base_url, url)
                items.append({"title": clean_title, "url": full_url})

            for value in data.values():
                if isinstance(value, (dict, list)):
                    self._extract_items_from_json(value, items)

        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    self._extract_items_from_json(item, items)

    def _parse_html_fragment(self, html_str, items):
        soup = BeautifulSoup(html_str, "lxml")
        for block in soup.find_all("div", class_="search-content-item"):
            a_tag = block.find("a")
            if not a_tag or not a_tag.get("href"):
                continue
            items.append({
                "title": a_tag.get_text(strip=True),
                "url": urljoin(self.base_url, a_tag["href"]),
            })

    def _parse_api_response(self, raw_text):
        text = raw_text.strip()
        json_str = text

        if text.startswith("jsonpCallback"):
            start = text.find("(")
            end = text.rfind(")")
            if start != -1 and end != -1:
                json_str = text[start + 1:end]

        page_items = []
        try:
            data = json.loads(json_str)
            if isinstance(data, dict) and any("<div" in str(v) for v in data.values()):
                for value in data.values():
                    if isinstance(value, str) and "<div" in value:
                        self._parse_html_fragment(value, page_items)
            elif isinstance(data, str) and "<div" in data:
                self._parse_html_fragment(data, page_items)
            else:
                self._extract_items_from_json(data, page_items)
        except json.JSONDecodeError:
            if "<div" in text or "<li" in text:
                self._parse_html_fragment(text, page_items)
            else:
                self.logger.warning(f"接口返回无法解析，已截断预览: {text[:180]}")

        uniq = []
        seen = set()
        for item in page_items:
            if item["url"] in seen:
                continue
            seen.add(item["url"])
            uniq.append(item)
        return uniq

    def run(self):
        self.logger.info(f"启动爬虫，目标: {self.start_url}")

        self.session.cookies.set("wdcid", uuid.uuid4().hex, domain="search.qstheory.cn")
        one_year_ago = self._one_year_ago_date()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Accept": "text/javascript, application/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://search.qstheory.cn",
            "Connection": "keep-alive",
        }

        total_saved = 0
        with DBManager() as db:
            for keyword in self.keywords:
                self.logger.info(f"开始搜索关键词：{keyword}")
                referer_url = f"{self.start_url}?keyword={quote(keyword)}"
                headers["Referer"] = referer_url

                try:
                    self.session.get(referer_url, headers={"User-Agent": headers["User-Agent"]}, timeout=config.REQUEST_TIMEOUT)
                    time.sleep(config.REQUEST_DELAY)
                except Exception as e:
                    self.logger.warning(f"预热请求失败（继续执行）: {e}")

                keyword_items = []
                seen_urls = set()

                for page in range(1, self.max_pages_per_keyword + 1):
                    searchword = (
                        f"(LinkTitle={keyword} or IntroTitle={keyword} or SubTitle={keyword}) "
                        f"AND PubTime >='{one_year_ago}'"
                    )
                    payload = {
                        "page": page,
                        "keyword": keyword,
                        "searchword": searchword,
                        "orderby": "RELEVANCE",
                    }

                    try:
                        res = self.session.post(
                            self.api_url,
                            data=payload,
                            headers=headers,
                            timeout=config.REQUEST_TIMEOUT,
                        )
                        res.encoding = "utf-8"
                        if res.status_code == 403 or "403 Forbidden" in res.text:
                            self.logger.warning(f"关键词 {keyword} 第 {page} 页触发 403，停止该关键词抓取")
                            break

                        current_page = self._parse_api_response(res.text)
                        added = 0
                        for item in current_page:
                            if item["url"] in seen_urls:
                                continue
                            seen_urls.add(item["url"])
                            keyword_items.append(item)
                            added += 1

                        self.logger.info(f"关键词 {keyword} 第 {page} 页新增 {added} 条")
                        if added == 0:
                            break

                        time.sleep(config.REQUEST_DELAY)
                    except Exception as e:
                        self.logger.error(f"关键词 {keyword} 第 {page} 页请求异常: {e}")
                        break

                self.logger.info(f"关键词 {keyword} 共发现 {len(keyword_items)} 条候选文章")

                for item in keyword_items:
                    if db.url_exists(self.name, item["url"]):
                        continue

                    try:
                        time.sleep(config.REQUEST_DELAY)
                        detail_res = self.session.get(
                            item["url"],
                            headers={"User-Agent": headers["User-Agent"]},
                            timeout=config.REQUEST_TIMEOUT,
                        )
                        detail_res.encoding = "utf-8"
                        full_data = self.parse_detail(detail_res, item)

                        full_data.update({
                            "source": self.name,
                            "url": item["url"],
                            "title_fp": sha1(full_data["title"]),
                            "content_fp": sha1(clean_text(full_data["content_text"])[:2000]),
                        })
                        db.save_article(full_data, self.categories)
                        total_saved += 1
                    except Exception as e:
                        self.logger.error(f"详情页抓取失败: {item['url']}，错误: {e}")

        self.logger.info(f"爬虫运行结束。本次新增入库: {total_saved}")
