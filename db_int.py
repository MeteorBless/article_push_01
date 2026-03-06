# -*- coding: utf-8 -*-
"""
初始化数据库：news.db
表结构与 Navicat 导出的 main.sql 保持一致（结构等价），并预置分类：
- 党建(dangjian)
- 时政(politics)
- 国际(world)
"""

import sqlite3

DB_PATH = "冗余/news.db"

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

-- ----------------------------
-- Table: articles
-- ----------------------------
CREATE TABLE IF NOT EXISTS "articles" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "source" TEXT NOT NULL,
  "url" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "pub_time" TEXT,
  "site_name" TEXT,
  "author" TEXT,
  "content_text" TEXT NOT NULL,
  "content_html" TEXT,
  "title_fp" TEXT,
  "content_fp" TEXT,
  "fetched_at" TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- ----------------------------
-- Table: categories
-- ----------------------------
CREATE TABLE IF NOT EXISTS "categories" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL,
  "slug" TEXT NOT NULL,
  UNIQUE ("name" ASC),
  UNIQUE ("slug" ASC)
);

-- ----------------------------
-- Table: article_category
-- ----------------------------
CREATE TABLE IF NOT EXISTS "article_category" (
  "article_id" INTEGER NOT NULL,
  "category_id" INTEGER NOT NULL,
  "created_at" TEXT NOT NULL DEFAULT (datetime('now','localtime')),
  PRIMARY KEY ("article_id", "category_id"),
  FOREIGN KEY ("article_id") REFERENCES "articles" ("id") ON DELETE CASCADE ON UPDATE NO ACTION,
  FOREIGN KEY ("category_id") REFERENCES "categories" ("id") ON DELETE CASCADE ON UPDATE NO ACTION
);

-- ----------------------------
-- Table: article_embeddings
-- ----------------------------
CREATE TABLE IF NOT EXISTS "article_embeddings" (
  "article_id" INTEGER,
  "model" TEXT NOT NULL,
  "embedding" TEXT NOT NULL,
  "dim" INTEGER NOT NULL,
  "created_at" TEXT DEFAULT (datetime('now','localtime')),
  PRIMARY KEY ("article_id")
);

-- ----------------------------
-- Table: dedup_runs
-- ----------------------------
CREATE TABLE IF NOT EXISTS "dedup_runs" (
  "run_id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "run_date" TEXT NOT NULL,
  "created_at" TEXT NOT NULL,
  "llm_model" TEXT NOT NULL,
  "candidate_file" TEXT NOT NULL,
  "note" TEXT
);

-- ----------------------------
-- Table: dedup_map
-- ----------------------------
CREATE TABLE IF NOT EXISTS "dedup_map" (
  "run_id" INTEGER NOT NULL,
  "article_id" INTEGER NOT NULL,
  "group_id" TEXT NOT NULL,
  "canonical_id" INTEGER NOT NULL,
  "decision" TEXT NOT NULL,
  "confidence" REAL,
  "reason" TEXT,
  "created_at" TEXT NOT NULL,
  PRIMARY KEY ("run_id", "article_id"),
  FOREIGN KEY ("run_id") REFERENCES "dedup_runs" ("run_id") ON DELETE NO ACTION ON UPDATE NO ACTION
);

-- ----------------------------
-- Table: candidate_groups
-- ----------------------------
CREATE TABLE IF NOT EXISTS "candidate_groups" (
  "run_date" TEXT NOT NULL,
  "kind" TEXT NOT NULL,
  "group_id" TEXT NOT NULL,
  "article_id" INTEGER NOT NULL,
  "created_at" TEXT NOT NULL,
  PRIMARY KEY ("run_date", "kind", "group_id", "article_id")
);

-- ----------------------------
-- Table: articles_deduped
-- ----------------------------
CREATE TABLE IF NOT EXISTS "articles_deduped" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "dedup_run_date" TEXT NOT NULL,
  "dedup_group_id" TEXT NOT NULL,
  "article_id" INTEGER NOT NULL,
  "title" TEXT NOT NULL,
  "url" TEXT,
  "source" TEXT,
  "pub_time" TEXT,
  "content_text" TEXT,
  "decision" TEXT NOT NULL,
  "confidence" REAL,
  "reason" TEXT,
  "created_at" TEXT NOT NULL
);

-- ----------------------------
-- Indexes
-- ----------------------------
CREATE INDEX IF NOT EXISTS "idx_article_category_article"
ON "article_category" ("article_id" ASC);

CREATE INDEX IF NOT EXISTS "idx_article_category_category"
ON "article_category" ("category_id" ASC);

CREATE INDEX IF NOT EXISTS "idx_articles_contentfp"
ON "articles" ("content_fp" ASC);

CREATE INDEX IF NOT EXISTS "idx_articles_source_pubtime"
ON "articles" ("source" ASC, "pub_time" ASC);

CREATE INDEX IF NOT EXISTS "idx_articles_source_url"
ON "articles" ("source" ASC, "url" ASC);

CREATE INDEX IF NOT EXISTS "idx_articles_titlefp"
ON "articles" ("title_fp" ASC);
"""

SEED_CATEGORIES = [
    ("党建", "dangjian"),
    ("时政", "politics"),
    ("国际", "world"),
]


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(DDL)

        # 预置分类
        conn.executemany(
            "INSERT OR IGNORE INTO categories (name, slug) VALUES (?, ?);",
            SEED_CATEGORIES
        )

        conn.commit()
        print(f"✅ 数据库初始化完成：{DB_PATH}")
        print("✅ 已预置分类：党建(dangjian)、时政(politics)、国际(world)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
