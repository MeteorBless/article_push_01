from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sqlite3
from collections import defaultdict
from typing import Optional, List, Dict, Any
from pathlib import Path
import contextlib

app = FastAPI(title="News Digest API")

# 获取当前脚本的绝对路径目录
BASE_DIR = Path(__file__).resolve().parent

# 使用绝对路径，防止 nohup 启动时工作目录不对导致找不到数据库
DB_PATH = BASE_DIR / "news.db"


def get_db_connection():
    # 增加 timeout 参数，防止写入时短暂锁库导致读取失败报错
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row

    # 开启 WAL 模式，极大提升 SQLite 的并发读写性能（写数据不阻塞读数据）
    conn.execute('PRAGMA journal_mode=WAL;')
    return conn


# 提供静态文件路由
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/")
async def serve_index():
    return FileResponse(BASE_DIR / "static/index.html")


@app.get("/api/dates")
async def get_available_dates():
    """获取数据库中所有存在的日期列表"""
    # 使用 contextlib.closing 确保无论是否发生异常，连接都会被关闭
    with contextlib.closing(get_db_connection()) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT run_date FROM digest_items ORDER BY run_date DESC")
        rows = cursor.fetchall()

    dates = [row['run_date'] for row in rows if row['run_date']]
    return {"dates": dates}


@app.get("/api/digest")
async def get_digest(date: Optional[str] = Query(None)):
    """
    获取新闻摘要。
    - 如果指定 date: 返回该日期的数据
    - 如果不指定 date: 返回最近 14 天的数据 (防止日积月累后全表扫描导致内存溢出)
    """
    with contextlib.closing(get_db_connection()) as conn:
        cursor = conn.cursor()

        sql = """
            SELECT run_date, item_no, topic, title, url, source, pub_time, summary, score
            FROM digest_items
        """
        params = []

        if date:
            sql += " WHERE run_date = ?"
            params.append(date)
        else:
            # 【重要防御】如果不传日期，只取最近 14 天的数据。你可以根据实际需求调整天数。
            # 这防止了几个月后，前端一次性拉取几万条数据直接把服务器内存搞崩。
            sql += " WHERE run_date >= date('now', 'localtime', '-14 days')"

        sql += " ORDER BY run_date DESC, topic, item_no ASC"

        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # 数据处理
    grouped_by_date = defaultdict(lambda: defaultdict(list))

    for row in rows:
        article = dict(row)
        r_date = article.pop("run_date")
        topic = article.pop("topic")

        if not r_date: continue

        grouped_by_date[r_date][topic].append(article)

    result_list = []
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

    # 【重要修复】关闭 reload=True，否则每天写入数据库文件变动时，服务会无限重启断开用户连接。
    # workers=1 (或者更高) 是生产环境更推荐的配置。
    uvicorn.run("homepage:app", host="0.0.0.0", port=8000, reload=False)