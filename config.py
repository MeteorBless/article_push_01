# -*- coding: utf-8 -*-
"""
【配置文件】
管理全局参数、数据库路径以及爬虫的开启/关闭状态。
"""
import os

# 数据库文件路径
DB_PATH = "F:\\papers\\codes\\article_push_01\\news.db"

# 网络请求相关配置
MAX_RETRIES = 3         # 请求失败重试次数
REQUEST_TIMEOUT = 30    # 请求超时时间（秒）
REQUEST_DELAY = 1.0     # 每次请求后的休眠时间（秒），防止被封

# 默认文章分类（如果爬虫类中未指定，则使用此分类）
# 注意：这些分类名称必须先在数据库的 categories 表中存在
DEFAULT_CATEGORIES = ["党建"]

# User-Agent 池（伪装浏览器身份）
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
]

# ==========================================
# 爬虫开关配置 (True=开启, False=关闭)
# 键名对应 spiders/ 下各爬虫类定义的 name 属性
# ==========================================
SPIDER_SETTINGS = {
    # "gmw": True,        # 光明网 党建 https://dangjian.gmw.cn/node_11941.htm
    # "people": True,     # 人民网 党建 http://dangjian.people.com.cn/GB/394443/
    # "chinanews": True,  # 中新网 国内时政 https://www.chinanews.com.cn/china.shtml
    # "xinhua": True,     # 新华网-习近平报道 附url https://www.news.cn/politics/leaders/xijinping/zxbd.html
    # "xinhua_zt": True,  # 新华网专题报道 树立和践行正确政绩观专题 https://www.news.cn/zt/slhjxzqzjg/zxbb.html
    # "qiushi_security": True,  # 求是网 搜索关键词为“安全” https://search.qstheory.cn/qiushi?keyword=%E5%AE%89%E5%85%A8&channelid=269025
    # "dangjian_theory": True,  # 党建网爬虫 http://www.dangjian.cn/jyjl/list_243366_1.html
    # "people_scitech": True,  # 人民网-科技频道 http://scitech.people.com.cn/
    "moe_briefing": True,  # 教育部简报爬虫 http://www.moe.gov.cn/jyb_sjzl/s3165

}