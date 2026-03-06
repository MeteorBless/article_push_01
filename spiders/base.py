# -*- coding: utf-8 -*-
"""
【爬虫基类】
定义所有爬虫的通用行为：
1. 网络请求（含重试、编码处理）
2. 调度流程（列表页 -> 详情页 -> 存储）
具体的解析逻辑由子类实现。
"""
import time
import logging
import requests
from abc import ABC, abstractmethod

import urllib3

from db_manager import DBManager
from utils import get_random_header, clean_text, sha1
import config

# 禁用因为 verify=False 而产生的安全警告日志 --->
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志输出格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)


class BaseSpider(ABC):
    name = "base"  # 爬虫唯一标识 (对应数据库 source 字段)
    start_url = ""  # 列表页入口 URL
    categories = []  # 该爬虫对应的文章分类

    def __init__(self):
        self.logger = logging.getLogger(self.name)
        self.session = requests.Session()
        # 如果子类没有定义分类，则使用配置文件的默认值
        if not self.categories:
            self.categories = config.DEFAULT_CATEGORIES

    def request(self, url):
        """封装的请求方法：包含重试机制、随机 UA、超时设置、编码自动修复"""
        for _ in range(config.MAX_RETRIES):
            try:
                response = self.session.get(
                    url,
                    headers=get_random_header(),
                    timeout=config.REQUEST_TIMEOUT,
                    verify=False  # <--- 必须要有这句，用来忽略新华网不合规的 SSL 证书
                )
                response.raise_for_status()

                # 针对部分老旧网站返回 ISO-8859-1 的乱码修正
                if not response.encoding or response.encoding.lower() == 'iso-8859-1':
                    response.encoding = response.apparent_encoding or 'utf-8'

                return response
            except Exception as e:
                self.logger.warning(f"请求失败: {url}, 错误: {e}, 正在重试...")
                time.sleep(1)
        return None

    @abstractmethod
    def parse_list(self, response):
        """
        【子类必须实现】解析列表页
        :param response: 列表页响应对象
        :return: 包含元数据的字典列表 [{'url':..., 'title':..., 'pub_date':...}]
        """
        pass

    @abstractmethod
    def parse_detail(self, response, item_meta):
        """
        【子类必须实现】解析详情页
        :param response: 详情页响应对象
        :param item_meta: 从列表页传来的元数据
        :return: 完整的文章数据字典
        """
        pass

    def run(self):
        """主执行逻辑：调度整个抓取流程"""
        self.logger.info(f"启动爬虫，目标: {self.start_url}")

        # 1. 请求列表页
        res_list = self.request(self.start_url)
        if not res_list:
            self.logger.error("列表页请求失败，任务终止")
            return

        # 2. 解析列表页
        try:
            items = self.parse_list(res_list)
        except Exception as e:
            self.logger.error(f"列表页解析异常: {e}")
            return

        self.logger.info(f"列表页解析完成，获取到 {len(items)} 条数据")

        with DBManager() as db:
            count = 0
            for item in items:
                url = item['url']

                # 3. 判重：如果数据库已有，则跳过
                if db.url_exists(self.name, url):
                    self.logger.info(f"跳过已存在: {item['title']}")
                    continue

                # 4. 请求详情页 (爬虫礼仪：暂停一会)
                time.sleep(config.REQUEST_DELAY)
                res_detail = self.request(url)
                if not res_detail:
                    continue

                try:
                    # 5. 解析详情页
                    full_data = self.parse_detail(res_detail, item)

                    # 补充系统字段 (source, hash 等)
                    full_data.update({
                        'source': self.name,
                        'url': url,
                        'title_fp': sha1(full_data['title']),
                        'content_fp': sha1(clean_text(full_data['content_text'])[:2000])
                    })

                    # 6. 保存到数据库
                    db.save_article(full_data, self.categories)
                    self.logger.info(f"入库成功: {full_data['title']}")
                    count += 1

                except Exception as e:
                    self.logger.error(f"详情页解析失败 {url}: {e}")

            self.logger.info(f"爬虫运行结束。本次新增入库: {count}")