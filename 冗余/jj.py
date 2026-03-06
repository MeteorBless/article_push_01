import requests
import re
import json
import pandas as pd
import datetime


# --- 核心爬虫函数 (保持不变，最稳的接口) ---
def get_fund_data_via_chart(fund_code):
    """
    通过“品种数据”接口获取全量历史净值
    """
    url = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
    print(f"正在获取基金 {fund_code} 的全量数据...")

    try:
        res = requests.get(url)
        res.encoding = "utf-8"
        text = res.text

        # 正则提取 Data_netWorthTrend (单位净值走势)
        pattern = r'var Data_netWorthTrend = (\[.*?\]);'
        match = re.search(pattern, text)

        if not match:
            print("未找到数据，请检查基金代码。")
            return None

        json_str = match.group(1)
        data_list = json.loads(json_str)

        processed_data = []
        for item in data_list:
            # 时间戳转日期
            timestamp = item['x'] / 1000
            date_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            processed_data.append({
                '净值日期': date_str,
                '单位净值': item['y'],
                '日增长率(%)': item['equityReturn']
            })

        df = pd.DataFrame(processed_data)
        # 确保日期列是时间格式，方便后续筛选
        df['净值日期'] = pd.to_datetime(df['净值日期'])
        # 按日期升序排列（旧 -> 新），方便计算
        df = df.sort_values(by='净值日期', ascending=True)
        return df

    except Exception as e:
        print(f"发生错误: {e}")
        return None


# --- 新增：回测计算函数 ---
def backtest_fund(df, start_date, end_date, amount):
    """
    回测特定时间段的收益
    :param df: 包含历史数据的DataFrame
    :param start_date: 买入日期 (字符串 '2023-01-01')
    :param end_date: 卖出日期 (字符串 '2024-01-01')
    :param amount: 买入金额 (浮点数)
    """
    # 1. 筛选时间区间
    s_date = pd.to_datetime(start_date)
    e_date = pd.to_datetime(end_date)

    # 筛选出 >= 开始日期 且 <= 结束日期 的数据
    mask = (df['净值日期'] >= s_date) & (df['净值日期'] <= e_date)
    sub_df = df.loc[mask]

    if sub_df.empty:
        print(f"错误：在 {start_date} 到 {end_date} 期间没有找到交易数据。")
        return

    # 2. 确定实际买入点和卖出点
    # 取区间内的第一天作为买入日（如果设定日期是周末，这里会自动取之后的第一个交易日）
    buy_info = sub_df.iloc[0]
    # 取区间内的最后一天作为卖出日
    sell_info = sub_df.iloc[-1]

    # 3. 计算收益
    buy_price = buy_info['单位净值']
    sell_price = sell_info['单位净值']

    # 份额 = 投入金额 / 买入净值
    share = amount / buy_price

    # 最终市值 = 份额 * 卖出净值
    final_value = share * sell_price

    # 收益金额
    profit = final_value - amount

    # 收益率
    yield_rate = (profit / amount) * 100

    # 格式化日期字符串，去掉 时:分:秒
    real_buy_date = buy_info['净值日期'].strftime('%Y-%m-%d')
    real_sell_date = sell_info['净值日期'].strftime('%Y-%m-%d')

    print("\n" + "=" * 40)
    print(f"   💰 基金回测报告 ({start_date} -> {end_date})")
    print("=" * 40)
    print(f"实际买入日期: {real_buy_date} | 净值: {buy_price:.4f}")
    print(f"实际卖出日期: {real_sell_date} | 净值: {sell_price:.4f}")
    print("-" * 40)
    print(f"初始本金: {amount:,.2f} 元")
    print(f"期末资产: {final_value:,.2f} 元")
    print("-" * 40)

    # --- 这一行是你要求的输出格式 ---
    print(f"若在 {real_buy_date} 买入 {int(amount)}，您最终收益为 {profit:+.2f}，收益率为 {yield_rate:.2f}%")
    print("=" * 40 + "\n")


# --- 主程序 ---
if __name__ == "__main__":
    # 1. 设置参数
    fund_code = "563690"  # 基金代码
    invest_amount = 400000  # 投资金额：40万
    start_time = "2025-01-01"  # 你想回测的开始时间
    end_time = "2026-01-01"  # 你想回测的结束时间

    # 2. 获取数据
    df = get_fund_data_via_chart(fund_code)

    # 3. 执行回测
    if df is not None:
        # 你可以多次调用回测，试不同的时间段
        backtest_fund(df, start_time, end_time, invest_amount)

        # 比如测试一下最近一个月的表现
        # backtest_fund(df, "2024-01-01", "2024-02-01", 400000)