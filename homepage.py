from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sqlite3
from collections import defaultdict
from typing import Optional, List, Dict, Any
from pathlib import Path

app = FastAPI(title="News Digest API")

DB_PATH = "news.db"

BASE_DIR = Path(__file__).resolve().parent


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# 提供静态文件路由
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_index():
    return FileResponse(BASE_DIR / "static/index.html")


@app.get("/api/dates")
async def get_available_dates():
    """获取数据库中所有存在的日期列表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT run_date FROM digest_items ORDER BY run_date DESC")
    rows = cursor.fetchall()
    conn.close()

    dates = [row['run_date'] for row in rows if row['run_date']]
    return {"dates": dates}


@app.get("/api/digest")
async def get_digest(date: Optional[str] = Query(None)):
    """
    获取新闻摘要。
    - 如果指定 date: 返回该日期的数据 (列表长度为1)。
    - 如果不指定 date: 返回所有日期的数据 (按日期降序)。
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
        SELECT run_date, item_no, topic, title, url, source, pub_time, summary, score
        FROM digest_items
    """
    params = []

    if date:
        sql += " WHERE run_date = ?"
        params.append(date)

    # 按日期降序，然后按 topic 和 item_no 排序
    sql += " ORDER BY run_date DESC, topic, item_no ASC"

    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()
    conn.close()

    # 数据处理：先按日期分组，再按 Topic 分组
    # 结构: { "2023-10-01": { "AI": [article, ...], "Politics": [...] } }
    grouped_by_date = defaultdict(lambda: defaultdict(list))

    for row in rows:
        article = dict(row)
        r_date = article.pop("run_date")  # 提取日期
        topic = article.pop("topic")  # 提取主题

        if not r_date: continue

        grouped_by_date[r_date][topic].append(article)

    # 转换为最终的前端友好列表格式
    # [ { "date": "2023-10-01", "topics": [ ... ] }, ... ]
    result_list = []

    # 保证日期顺序 (虽然 SQL 排过序，但字典是无序的，所以依赖 SQL 结果的插入顺序或重新排序)
    # 这里我们重新按 key (日期) 降序排序确保万无一失
    sorted_dates = sorted(grouped_by_date.keys(), reverse=True)

    for d in sorted_dates:
        topics_map = grouped_by_date[d]
        topics_list = []
        for topic, articles in topics_map.items():
            topics_list.append({
                "topic": topic,
                "count": len(articles),
                "articles": articles
            })

        # 对当天的 Topic 按文章数降序排列
        topics_list.sort(key=lambda x: x["count"], reverse=True)

        result_list.append({
            "date": d,
            "topics": topics_list
        })

    return result_list


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("homepage:app", host="0.0.0.0", port=8000, reload=True)