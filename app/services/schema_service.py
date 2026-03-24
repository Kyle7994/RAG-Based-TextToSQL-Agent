# schema_service.py
from app.services.mysql_service import get_conn

def extract_schema_chunks():
    conn = get_conn()
    chunks = []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                ORDER BY table_name
            """)
            rows = cur.fetchall()

        table_map = {}
        for table, col, dtype in rows:
            table_map.setdefault(table, []).append((col, dtype))

        for table, cols in table_map.items():
            content = f"Table: {table}\nColumns:\n"
            for col, dtype in cols:
                content += f"- {col} ({dtype})\n"

            chunks.append({
                "table": table,
                "content": content
            })

        return chunks
    finally:
        conn.close()