import sqlite3
from pathlib import Path

SRC = "冗余/news.db"
DST = "news2.db"

QUERY = """
SELECT type, name, tbl_name, sql
FROM sqlite_master
WHERE sql IS NOT NULL
  AND name NOT LIKE 'sqlite_%'
ORDER BY
  CASE type
    WHEN 'table' THEN 1
    WHEN 'index' THEN 2
    WHEN 'view' THEN 3
    WHEN 'trigger' THEN 4
    ELSE 9
  END,
  name;
"""

def export_schema_sql(src_db: str) -> str:
    conn = sqlite3.connect(src_db)
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    conn.close()

    lines = [
        "PRAGMA foreign_keys=ON;",
        "BEGIN;",
    ]
    for obj_type, name, tbl_name, sql in rows:
        lines.append(sql.rstrip(";") + ";")
    lines.append("COMMIT;")
    return "\n\n".join(lines)

def init_empty_db_from_schema(schema_sql: str, dst_db: str):
    # 确保是全新库
    Path(dst_db).unlink(missing_ok=True)

    conn = sqlite3.connect(dst_db)
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    schema_sql = export_schema_sql(SRC)
    init_empty_db_from_schema(schema_sql, DST)
    print("OK: 已从 news.db 复制结构到 news2.db（不含数据）")
