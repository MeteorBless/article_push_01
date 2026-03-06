# -*- coding: utf-8 -*-
"""
【程序入口】
1. 读取 config.py 中的开关配置 (SPIDER_SETTINGS)。
2. 动态加载并实例化开启的爬虫类。
3. 依次执行爬取任务。
"""
import config
from spiders.chinanews_spider import ChinaNewsSpider
from spiders.dangjian_spider import DangjianTheorySpider
from spiders.gm_spider import GuangmingSpider
from spiders.moe_spider import MoeBriefingSpider
from spiders.people_scitech_spider import PeopleSciTechSpider
from spiders.rm_spider import RenminSpider
from spiders.xinhua_spider import XinhuaSpider
from spiders.xinhua_zt_spider import XinhuaZtSpider
from spiders.qiushi_theory_spider import QiushiTheorySpider

# 爬虫映射表：配置文件中的键名 -> 对应的爬虫类
SPIDER_MAPPING = {
    "gmw": GuangmingSpider,     # 光明网
    "people": RenminSpider,     # 人民网
    "chinanews": ChinaNewsSpider,   # 中新网
    "xinhua": XinhuaSpider,      # 新华网总书记专题
    "xinhua_zt": XinhuaZtSpider,  # 新华网专题爬虫
    "qiushi_security": QiushiTheorySpider,  # 求是网国家安全主题
    "dangjian_theory": DangjianTheorySpider,  # 党建网
    "people_scitech": PeopleSciTechSpider, # 人民网科技
    "moe_briefing": MoeBriefingSpider  # 教育部简报爬虫
}


def main():
    print("=" * 40)
    print("🚀 爬虫调度程序启动")
    print("=" * 40)

    run_count = 0

    # 遍历配置字典
    for spider_name, is_enabled in config.SPIDER_SETTINGS.items():
        # 1. 检查开关是否为 True
        if not is_enabled:
            print(f"⚪ [跳过] {spider_name} (配置为关闭)")
            continue

        # 2. 检查映射表中是否存在该爬虫
        if spider_name not in SPIDER_MAPPING:
            print(f"⚠️ [警告] 配置了未知的爬虫名称 '{spider_name}'，已跳过。")
            continue

        # 3. 实例化并运行
        spider_class = SPIDER_MAPPING[spider_name]
        try:
            print(f"\n▶ 正在运行: {spider_name} ...")
            spider_instance = spider_class()
            spider_instance.run()
            run_count += 1
        except Exception as e:
            print(f"❌ [错误] 爬虫 {spider_name} 发生异常崩溃: {e}")

    print("\n" + "=" * 40)
    if run_count == 0:
        print("⚠️ 没有爬虫被执行，请检查 config.py 中的 SPIDER_SETTINGS。")
    else:
        print(f"✅ 执行结束，共运行了 {run_count} 个爬虫任务。")
    print("=" * 40)


if __name__ == "__main__":
    main()