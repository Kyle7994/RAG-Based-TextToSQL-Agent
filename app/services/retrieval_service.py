# retrieval_service.py
import psycopg
from app.services.embedding_service import get_embedding
from app.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

async def index_schema(chunks):
    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

    try:
        with conn.cursor() as cur:
            for c in chunks:
                emb = await get_embedding(c["content"])

                cur.execute("""
                    INSERT INTO schema_chunks (chunk_type, source_name, content, metadata, embedding)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    "table",
                    c["table"],
                    c["content"],
                    "{}",
                    emb
                ))
        conn.commit()
    finally:
        conn.close()