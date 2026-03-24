# postgres_service.py

import json

import psycopg
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from app.config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=5),
    retry=retry_if_exception_type((psycopg.OperationalError,)),
    reraise=True,
)
def get_pg_conn():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        autocommit=True,
    )


def clear_and_save_schema_chunks(chunks: list[dict]):
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE schema_chunks;")
            for chunk in chunks:
                cur.execute(
                    """
                    INSERT INTO schema_chunks
                    (chunk_type, source_name, content, metadata, embedding)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        chunk["chunk_type"],
                        chunk["source_name"],
                        chunk["content"],
                        json.dumps(chunk["metadata"]),
                        chunk["embedding"],
                    ),
                )


def search_schema_chunks(query_embedding: list[float], limit: int = 3) -> list[str]:
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            cur.execute(
                """
                SELECT content, metadata
                FROM schema_chunks
                ORDER BY embedding <-> %s::vector
                LIMIT %s
                """,
                (embedding_str, limit),
            )
            rows = cur.fetchall()
            return [row[0] for row in rows]


def save_sql_example(question: str, sql_text: str, embedding: list[float]):
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sql_examples (question, sql_text, features, embedding)
                VALUES (%s, %s, %s, %s)
                """,
                (question, sql_text, json.dumps({"validated": True}), embedding),
            )


def search_sql_examples(query_embedding: list[float], limit: int = 2) -> list[dict]:
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            cur.execute(
                """
                SELECT question, sql_text
                FROM sql_examples
                ORDER BY embedding <-> %s::vector
                LIMIT %s
                """,
                (embedding_str, limit),
            )
            rows = cur.fetchall()
            return [{"question": row[0], "sql": row[1]} for row in rows]