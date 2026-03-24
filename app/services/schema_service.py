# # -*- coding: utf-8 -*-

# """
# app/services/schema_service.py

# This module is responsible for synchronizing the database schema from the
# source MySQL database to the PostgreSQL vector store.

# The process involves:
# 1. Introspecting the MySQL database to extract table and column information.
# 2. Structuring this information into text "chunks," typically one per table.
# 3. Generating a vector embedding for each chunk to capture its semantic meaning.
# 4. Storing these chunks and their embeddings in PostgreSQL for retrieval.
# 5. Computing a version hash of the entire schema and storing it in Redis
#    to be used for cache invalidation.
# """

# from app.services.mysql_service import get_conn
# from app.services.embedding_service import get_embedding
# from app.services.postgres_service import clear_and_save_schema_chunks
# from app.services.redis_service import set_current_schema_version
# from app.config import MYSQL_DB
# import hashlib


# def compute_schema_version(schema_text: str) -> str:
#     """
#     Computes a stable SHA256 hash of the full schema text to create a version identifier.

#     This version is crucial for cache invalidation. If the schema changes, the
#     version hash will change, and all cached responses associated with the old

#     schema will become invalid.

#     Args:
#         schema_text (str): A string representing the entire database schema.

#     Returns:
#         str: A 16-character hexadecimal string representing the schema version.
#     """
#     return hashlib.sha256(schema_text.encode("utf-8")).hexdigest()[:16]


# async def sync_mysql_schema_to_pg():
#     """
#     Orchestrates the entire schema synchronization process.

#     This is the main function that reads the schema from MySQL, processes it,
#     generates embeddings, and saves the results to PostgreSQL and Redis.

#     Returns:
#         A dictionary summarizing the result of the operation, including the
#         number of tables synced and the new schema version.
#     """
#     conn = get_conn()
#     try:
#         with conn.cursor() as cur:
#             # 1. Extract raw table and column information from MySQL's information schema.
#             cur.execute("""
#                 SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
#                 FROM information_schema.COLUMNS
#                 WHERE TABLE_SCHEMA = %s
#                 ORDER BY TABLE_NAME, ORDINAL_POSITION
#             """, (MYSQL_DB,))
#             rows = cur.fetchall()

#             # Group columns by table name.
#             tables = {}
#             for table_name, column_name, data_type in rows:
#                 if table_name not in tables:
#                     tables[table_name] = []
#                 tables[table_name].append(f"{column_name} ({data_type})")

#         # 2. Construct a single, stable string representation of the entire schema.
#         # Sorting by table name ensures that the hash is deterministic.
#         all_schema_text_parts = []
#         for table_name in sorted(tables.keys()):
#             columns_text = ", ".join(tables[table_name])
#             all_schema_text_parts.append(f"Table: {table_name}\\nColumns: {columns_text}")

#         full_schema_text = "\\n\\n".join(all_schema_text_parts)
#         schema_version = compute_schema_version(full_schema_text)

#         # 3. Create schema "chunks" (one per table) and generate embeddings.
#         chunks = []
#         for table_name in sorted(tables.keys()):
#             # Each chunk is a text description of a single table.
#             content = f"Table: {table_name}\\nColumns: {', '.join(tables[table_name])}"
#             embedding = await get_embedding(content)

#             chunks.append({
#                 "chunk_type": "table_schema",
#                 "source_name": table_name,
#                 "content": content,
#                 "metadata": {
#                     "table": table_name,
#                     "schema_version": schema_version
#                 },
#                 "embedding": embedding
#             })

#         # 4. Atomically clear old schema chunks and save the new ones to PostgreSQL.
#         if chunks:
#             clear_and_save_schema_chunks(chunks)

#         # 5. Update the global schema version in Redis to invalidate caches.
#         set_current_schema_version(schema_version)

#         return {
#             "status": "success",
#             "tables_synced": len(chunks),
#             "schema_version": schema_version
#         }

#     finally:
#         # Ensure the database connection is always closed.
#         conn.close()
# -*- coding: utf-8 -*-

"""
app/services/schema_service.py

This module is responsible for synchronizing the database schema from the
source MySQL database to the PostgreSQL vector store.

Enhanced version:
1. Introspect MySQL schema.
2. Load dictionary.yaml as semantic annotations.
3. Merge schema + dictionary into table-level chunks.
4. Generate embeddings for richer chunks.
5. Store chunks in PostgreSQL vector store.
6. Compute schema version from merged content and store in Redis.
"""

from app.services.mysql_service import get_conn
from app.services.embedding_service import get_embedding
from app.services.postgres_service import clear_and_save_schema_chunks
from app.services.redis_service import set_current_schema_version
from app.config import MYSQL_DB

import hashlib
import os
import yaml


DICTIONARY_PATH = os.getenv("SCHEMA_DICTIONARY_PATH", "dictionary.yaml")


def compute_schema_version(schema_text: str) -> str:
    """
    Computes a stable SHA256 hash of the full schema text to create a version identifier.
    """
    return hashlib.sha256(schema_text.encode("utf-8")).hexdigest()[:16]


def load_dictionary(dictionary_path: str = DICTIONARY_PATH) -> dict:
    """
    Load dictionary.yaml.

    Expected format:
    tables:
      users:
        email: ...
        country: ...
      orders:
        status: ...

    Returns:
        dict: Parsed dictionary. If file does not exist, returns {}.
    """
    if not os.path.exists(dictionary_path):
        return {}

    with open(dictionary_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        return {}

    return data


def build_table_chunk_content(
    table_name: str,
    columns: list[tuple[str, str]],
    dictionary_tables: dict,
) -> str:
    """
    Build one richer text chunk for a table by merging schema and dictionary.

    Example output:

    Table: users
    Columns:
    - id (bigint)
    - email (varchar): Text field containing email addresses of users.
    - country (varchar): Categorical field. Observed values include: AU, BR, CA, DE, FR.
    - is_vip (tinyint): Boolean flag indicating whether the user is VIP.
    """
    table_dict = dictionary_tables.get(table_name, {}) if isinstance(dictionary_tables, dict) else {}

    lines = [f"Table: {table_name}", "Columns:"]

    for column_name, data_type in columns:
        description = table_dict.get(column_name)

        if description:
            lines.append(f"- {column_name} ({data_type}): {description}")
        else:
            lines.append(f"- {column_name} ({data_type})")

    return "\n".join(lines)


async def sync_mysql_schema_to_pg():
    """
    Orchestrates the entire schema synchronization process.

    Flow:
    - Read schema from MySQL
    - Load dictionary.yaml
    - Merge schema + dictionary into table chunks
    - Generate embeddings
    - Save to PostgreSQL
    - Save schema version to Redis
    """
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                (MYSQL_DB,),
            )
            rows = cur.fetchall()

        # Group columns by table name, preserving order.
        # tables = {
        #   "users": [("id", "bigint"), ("email", "varchar"), ...],
        #   "orders": [("id", "bigint"), ("status", "varchar"), ...]
        # }
        tables: dict[str, list[tuple[str, str]]] = {}
        for table_name, column_name, data_type in rows:
            tables.setdefault(table_name, []).append((column_name, data_type))

        # Load semantic dictionary
        dictionary = load_dictionary()
        dictionary_tables = dictionary.get("tables", {}) if isinstance(dictionary, dict) else {}

        # Build full merged schema text for hashing
        all_schema_text_parts = []
        for table_name in sorted(tables.keys()):
            content = build_table_chunk_content(
                table_name=table_name,
                columns=tables[table_name],
                dictionary_tables=dictionary_tables,
            )
            all_schema_text_parts.append(content)

        full_schema_text = "\n\n".join(all_schema_text_parts)
        schema_version = compute_schema_version(full_schema_text)

        # Build chunks and embeddings
        chunks = []
        for table_name in sorted(tables.keys()):
            content = build_table_chunk_content(
                table_name=table_name,
                columns=tables[table_name],
                dictionary_tables=dictionary_tables,
            )

            embedding = await get_embedding(content)

            chunks.append(
                {
                    "chunk_type": "table_schema",
                    "source_name": table_name,
                    "content": content,
                    "metadata": {
                        "table": table_name,
                        "schema_version": schema_version,
                        "has_dictionary": table_name in dictionary_tables,
                    },
                    "embedding": embedding,
                }
            )

        # Save chunks
        if chunks:
            clear_and_save_schema_chunks(chunks)

        # Update global schema version
        set_current_schema_version(schema_version)

        return {
            "status": "success",
            "tables_synced": len(chunks),
            "schema_version": schema_version,
            "dictionary_loaded": bool(dictionary_tables),
        }

    finally:
        conn.close()