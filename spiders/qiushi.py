import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import urllib.parse
import uuid

# ================= 配置区域 =================
KEYWORDS = ["安全", "高质量发展", "中国式现代化"]

MAX_PAGES_PER_KEYWORD = 3

# ============================================

def get_one_year_ago_date():
    now = datetime.now()
    return f"{now.year - 1}.{now.month}.{now.day}"


def extract_items_from_json(data, items_list, base_url="https://www.qstheory.cn"):
    if isinstance(data, dict):
        url = data.get('url') or data.get('LinkUrl') or data.get('docpuburl') or data.get('docUrl')
        title = data.get('title') or data.get('LinkTitle') or data.get('IntroTitle') or data.get('doctitle')

        if url and isinstance(url, str):
            clean_title = BeautifulSoup(title, "html.parser").get_text(strip=True) if title else "未知标题"
            full_url = urllib.parse.urljoin(base_url, url)
            items_list.append({'title': clean_title, 'link': full_url})

        for value in data.values():
            if isinstance(value, (dict, list)):
                extract_items_from_json(value, items_list, base_url)

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                extract_items_from_json(item, items_list, base_url)


def parse_html_fragment(html_str, items_list, base_url="https://www.qstheory.cn"):
    """HTML 片段硬解"""
    soup = BeautifulSoup(html_str, 'html.parser')
    for item in soup.find_all('div', class_='search-content-item'):
        a_tag = item.find('a')
        if a_tag and a_tag.get('href'):
            link = urllib.parse.urljoin(base_url, a_tag['href'])
            title = a_tag.get_text(strip=True)
            items_list.append({'title': title, 'link': link})


def scrape_qstheory():
    api_url = "https://search.qstheory.cn/qiushi/moreNew?callback=jsonpCallback"
    session = requests.Session()


    session.cookies.set("wdcid", uuid.uuid4().hex, domain="search.qstheory.cn")

    one_year_ago = get_one_year_ago_date()

    for keyword in KEYWORDS:
        print(f"\n{'=' * 20} 开始搜索关键字: 【{keyword}】 {'=' * 20}")
        news_list = []
        seen_urls = set()

        referer_url = f"https://search.qstheory.cn/qiushi?keyword={urllib.parse.quote(keyword)}"


        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://search.qstheory.cn",
            "Referer": referer_url,
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"'
        }



        print("  hahaha...")
        try:
            session.get(referer_url, headers={"User-Agent": headers["User-Agent"]})
            time.sleep(1.5)  # 停留 1.5 秒
        except:
            pass

        for page in range(1, MAX_PAGES_PER_KEYWORD + 1):
            print(f"正在获取【{keyword}】的第 {page} 页...")

            searchword = f"(LinkTitle={keyword} or IntroTitle={keyword} or SubTitle={keyword}) AND PubTime >='{one_year_ago}'"
            payload_dict = {
                "page": page,
                "keyword": keyword,
                "searchword": searchword,
                "orderby": "RELEVANCE"
            }
            payload_str = urllib.parse.urlencode(payload_dict)

            try:
                res = session.post(api_url, data=payload_str, headers=headers)
                res.encoding = 'utf-8'
                text = res.text.strip()

                if res.status_code == 403 or '403 Forbidden' in text:
                    print(f"\n❌ ！")
                    print(
                        f"hahaa")
                    break

                # 剥离 JSONP 外壳
                json_str = text
                if text.startswith("jsonpCallback"):
                    start = text.find('(')
                    end = text.rfind(')')
                    if start != -1 and end != -1:
                        json_str = text[start + 1:end]

                current_page_items = []

                try:
                    data = json.loads(json_str)
                    if isinstance(data, dict) and any('<div' in str(v) for v in data.values()):
                        for v in data.values():
                            if isinstance(v, str) and '<div' in v:
                                parse_html_fragment(v, current_page_items)
                    elif isinstance(data, str) and '<div' in data:
                        parse_html_fragment(data, current_page_items)
                    else:
                        extract_items_from_json(data, current_page_items)

                except json.JSONDecodeError:
                    if '<div' in text or '<li' in text:
                        parse_html_fragment(text, current_page_items)
                    else:
                        print(f"  -> 解析失败！预览:\n{text[:200]}")
                        break

                new_added = 0
                for item in current_page_items:
                    if item['link'] not in seen_urls:
                        seen_urls.add(item['link'])
                        news_list.append(item)
                        new_added += 1

                if new_added == 0:
                    print("  -> 本页没有新数据，可能已到底。")
                    break

                time.sleep(1.5)  # 降低翻页频率防封

            except Exception as e:
                print(f"  -> 请求接口发生异常: {e}")
                break

        print(f"关键字【{keyword}】共找到 {len(news_list)} 篇文章，开始抓取正文...")

        output_filename = f"qstheory_{keyword}_results.txt"

        with open(output_filename, "w", encoding="utf-8") as f:
            for index, news in enumerate(news_list):
                print(f"[{index + 1}/{len(news_list)}] 正在抓取: {news['title']}")
                try:
                    detail_res = session.get(news['link'], headers={"User-Agent": headers["User-Agent"]})
                    detail_res.encoding = 'utf-8'
                    article_soup = BeautifulSoup(detail_res.text, 'html.parser')

                    content_box = article_soup.find('div', id='detailContent') or \
                                  article_soup.find('div', class_='highlight') or \
                                  article_soup.find('div', class_='text')

                    if content_box:
                        paragraphs = [p.get_text(strip=True) for p in content_box.find_all('p')]
                        content = '\n'.join(paragraphs)
                    else:
                        content = "未找到正文内容，可能是视频页或特殊排版。"

                    news['content'] = content

                    f.write(f"标题: {news['title']}\n链接: {news['link']}\n正文:\n{content}\n")
                    f.write("=" * 80 + "\n\n")

                    preview = content[:40].replace('\n', '') + "..." if len(content) > 40 else content
                    print(f"   -> 成功 (预览: {preview})")
                    time.sleep(1)

                except Exception as e:
                    print(f"   -> 抓取正文失败: {e}")
                    f.write(f"标题: {news['title']}\n链接: {news['link']}\n抓取正文失败: {e}\n")
                    f.write("=" * 80 + "\n\n")

        print(f"【{keyword}】的数据已保存至 {output_filename}\n")

    print("所有关键字爬取任务已完成！")


if __name__ == "__main__":
    scrape_qstheory()