# -*- coding: utf-8 -*-

"""
app/services/postgres_service.py

This module manages all interactions with the PostgreSQL database, which serves
as the vector store for the RAG (Retrieval-Augmented Generation) system.

It handles:
- Establishing resilient connections to the PostgreSQL server.
- Storing and retrieving vectorized representations of database schema information.
- Storing and retrieving vectorized few-shot examples (question-SQL pairs).
- Performing vector similarity searches to find contextually relevant information
  to augment the LLM prompts.
"""

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
    """
    Establishes and returns a connection to the PostgreSQL database.

    Includes a retry mechanism to handle transient connection errors.

    Returns:
        psycopg.Connection: A database connection object.
    """
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        autocommit=True,
    )


def clear_and_save_schema_chunks(chunks: list[dict]):
    """
    Clears the existing schema information and saves new, chunked schema data.

    This function is a key part of the schema synchronization process. It first
    truncates the `schema_chunks` table to ensure freshness and then inserts
    the new chunks, each with its corresponding embedding.

    Args:
        chunks (list[dict]): A list of schema chunk dictionaries, each expected
                             to have 'chunk_type', 'source_name', 'content',
                             'metadata', and 'embedding'.
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # TRUNCATE is used for a fast, complete clear-down of the table.
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


def search_schema_chunks(query_embedding: list[float], limit: int = 5) -> list[str]:
    """
    Searches for the most relevant schema chunks based on a query embedding.

    This performs a vector similarity search (cosine distance) in the `schema_chunks`
    table to find the schema definitions that are semantically closest to the user's question.

    Args:
        query_embedding (list[float]): The vector embedding of the user's question.
        limit (int): The maximum number of schema chunks to retrieve.

    Returns:
        list[str]: A list of the content of the most relevant schema chunks.
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            # The <-> operator from pgvector performs a cosine distance search.
            embedding_str = "[" + ",".join(map(str, query_embedding)) + "]"
            cur.execute(
                """
                SELECT content
                FROM schema_chunks
                ORDER BY embedding <-> %s::vector
                LIMIT %s
                """,
                (embedding_str, limit),
            )
            rows = cur.fetchall()
            return [row[0] for row in rows]


def save_sql_example(question: str, sql_text: str, embedding: list[float]):
    """
    Saves a new validated question-SQL pair as a few-shot example.

    These examples are stored in the `sql_examples` table and are used to improve
    the LLM's accuracy by providing it with relevant samples.

    Args:
        question (str): The natural language question.
        sql_text (str): The corresponding, validated SQL query.
        embedding (list[float]): The vector embedding of the question.
    """
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
    """
    Searches for the most similar few-shot examples based on a query embedding.

    This performs a vector similarity search in the `sql_examples` table to find
    question-SQL pairs that are semantically similar to the current user question.

    Args:
        query_embedding (list[float]): The vector embedding of the user's question.
        limit (int): The maximum number of examples to retrieve.

    Returns:
        list[dict]: A list of dictionaries, each containing a 'question' and 'sql' pair.
    """
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
