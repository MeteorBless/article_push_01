# -*- coding: utf-8 -*-
"""
【数据库模块】
封装 SQLite 操作，实现上下文管理器（with语句），负责数据查重和入库。
"""
import sqlite3
from config import DB_PATH

class DBManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        """进入 with 语句时自动连接数据库"""
        self.conn = sqlite3.connect(self.db_path)
        # 设置 row_factory 可以通过列名访问数据（虽然这里主要做插入）
        self.conn.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 语句时自动提交或回滚，并关闭连接"""
        if exc_type:
            self.conn.rollback() # 发生异常回滚
        else:
            self.conn.commit()   # 正常结束提交
        self.conn.close()

    def url_exists(self, source, url):
        """检查某来源的 URL 是否已存在于数据库，防止重复抓取"""
        cursor = self.conn.execute(
            "SELECT 1 FROM articles WHERE source=? AND url=? LIMIT 1",
            (source, url)
        )
        return cursor.fetchone() is not None

    def _get_category_ids(self, names):
        """根据分类名称查询对应的 ID，如果分类不存在则报错"""
        ids = []
        for name in names:
            cur = self.conn.execute("SELECT id FROM categories WHERE name=? LIMIT 1", (name,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"数据库错误：分类 '{name}' 不存在，请先在 categories 表中手动插入该分类。")
            ids.append(int(row[0]))
        return ids

    def save_article(self, data, category_names):
        """
        核心入库逻辑：
        1. 获取分类 ID
        2. 插入文章基本信息到 articles 表
        3. 插入关联关系到 article_category 表
        """
        # 1. 预检分类 ID
        cat_ids = self._get_category_ids(category_names)

        # 2. 插入文章
        # 使用 ? 占位符防止 SQL 注入
        cursor = self.conn.execute(
            """
            INSERT INTO articles
            (source, url, title, pub_time, site_name, author,
             content_text, content_html, title_fp, content_fp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["source"],
                data["url"],
                data["title"],
                data.get("pub_time", ""),
                data.get("site_name", ""),
                data.get("author", ""),
                data["content_text"],
                data.get("content_html", ""),
                data["title_fp"],
                data["content_fp"],
            ),
        )
        article_id = cursor.lastrowid

        # 3. 关联分类
        for cid in cat_ids:
            self.conn.execute(
                "INSERT OR IGNORE INTO article_category (article_id, category_id) VALUES (?, ?)",
                (article_id, cid),
            )