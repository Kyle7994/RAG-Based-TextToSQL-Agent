#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app/scripts/auto_profiler.py

This script automatically generates a data dictionary for a MySQL database.

It inspects the database schema, samples data from relevant columns,
and uses a combination of heuristics and a Large Language Model (LLM) to
generate descriptive comments for each column. The final output is a
YAML file (`dictionary.yaml`) that can be used as a reference for
understanding the database schema.

This is intended to be run as a standalone script.
"""

import asyncio
import json
import re
from decimal import Decimal
from typing import Any

import httpx
import pymysql
import yaml
from tqdm import tqdm

# Import configuration from the main application
from app.config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB,
    LLM_BASE_URL, LLM_MODEL,
    HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT, HTTP_WRITE_TIMEOUT, HTTP_POOL_TIMEOUT,
)


# Defines the expected JSON schema for the LLM's response.
COMMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "comment": {"type": "string"}
    },
    "required": ["comment"]
}


def get_mysql_conn():
    """
    Establishes and returns a new connection to the MySQL database.

    Uses connection details from the application's configuration.

    Returns:
        pymysql.Connection: A database connection object.
    """
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=True,
    )


def safe_dump_yaml(data: dict[str, Any], path: str) -> None:
    """
    Safely dumps a dictionary to a YAML file with UTF-8 encoding.

    Args:
        data (dict): The dictionary to write.
        path (str): The path to the output YAML file.
    """
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def normalize_sample(v: Any) -> Any:
    """
    Normalizes a database sample value for processing.
    Currently, it converts Decimal types to strings.

    Args:
        v: The sample value.

    Returns:
        The normalized value.
    """
    if isinstance(v, Decimal):
        return str(v)
    return v


def should_skip_column(column: str, data_type: str) -> bool:
    """
    Determines if a column should be skipped based on its name and data type.
    This helps focus the profiling effort on columns that are more likely to
    contain meaningful business context.

    Args:
        column (str): The name of the column.
        data_type (str): The data type of the column.

    Returns:
        bool: True if the column should be skipped, False otherwise.
    """
    c = column.lower()
    d = data_type.lower()

    # Skip common primary/foreign keys and timestamp columns
    if c in {"id", "created_at", "updated_at", "deleted_at"}:
        return True

    if c.endswith("_id"):
        return True

    # Focus on high-value, human-readable data types for now
    if d not in {"varchar", "char", "enum", "boolean", "tinyint"}:
        return True

    return False


def heuristic_comment(table: str, column: str, data_type: str, samples: list[Any]) -> str:
    """
    Generates a simple, rule-based comment for a column.
    This provides a baseline comment without needing to call an LLM.

    Args:
        table (str): The name of the table.
        column (str): The name of the column.
        data_type (str): The data type of the column.
        samples (list[Any]): A list of sample values from the column.

    Returns:
        str: A heuristically generated comment, or an empty string if no rule matches.
    """
    c = column.lower()
    d = data_type.lower()
    cleaned = [normalize_sample(x) for x in samples if x is not None]

    if c.startswith("is_") or d == "boolean":
        return f"Boolean flag for {column.replace('_', ' ')}"

    if c in {"status", "channel", "country", "category"} and cleaned:
        values = ", ".join(map(str, cleaned[:5]))
        return f"Categorical field; observed values: {values}"

    return ""


def extract_json_object(text: str) -> dict[str, Any] | None:
    """
    Robustly extracts a JSON object from a string, which might be malformed
    or contain surrounding text.

    Args:
        text (str): The input string from the LLM.

    Returns:
        dict | None: The parsed JSON object, or None if parsing fails.
    """
    text = text.strip()

    # First, try to parse the whole string as JSON
    try:
        return json.loads(text)
    except Exception:
        pass

    # If that fails, find the first valid JSON object within the string
    match = re.search(r"\{.*?\}", text, re.DOTALL)
    if not match:
        return None

    try:
        return json.loads(match.group(0))
    except Exception:
        return None


async def ask_llm_for_comment(
    client: httpx.AsyncClient,
    table: str,
    column: str,
    data_type: str,
    samples: list[Any],
) -> str:
    """
    Asks the configured Large Language Model to generate a comment for a database column.

    It constructs a detailed prompt including the table, column, data type, and sample values.

    Args:
        client (httpx.AsyncClient): The HTTP client for making the request.
        table (str): The table name.
        column (str): The column name.
        data_type (str): The column's data type.
        samples (list[Any]): Sample data from the column.

    Returns:
        str: The comment generated by the LLM, or an empty string on failure.
    """
    clean_samples = [normalize_sample(s) for s in samples]

    prompt = f"""
You are generating a short database dictionary entry.

Return exactly one JSON object matching this schema:
{json.dumps(COMMENT_SCHEMA, ensure_ascii=False)}

Table: {table}
Column: {column}
Data type: {data_type}
Sample values: {json.dumps(clean_samples, ensure_ascii=False)}

Rules:
- Max 16 words.
- Be factual only.
- Do not infer business KPIs or hidden semantics.
- If uncertain, describe it generically.
""".strip()

    try:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "format": COMMENT_SCHEMA,
                "options": {
                    "temperature": 0
                }
            },
        )
        resp.raise_for_status()

        raw_content = resp.json().get("message", {}).get("content", "").strip()
        if not raw_content:
            return ""

        parsed = extract_json_object(raw_content)
        if not parsed:
            return ""

        comment = parsed.get("comment", "")
        if not isinstance(comment, str):
            return ""

        return comment.strip()

    except Exception as e:
        print(f"\\n[LLM Error] {table}.{column}: {type(e).__name__}: {e}")
        return ""


def load_candidate_columns():
    """
    Connects to the database and retrieves a list of columns to be profiled.

    It filters out columns that are unlikely to be interesting and fetches
    distinct sample values for the remaining ones.

    Returns:
        list: A list of tuples, each containing (table, column, data_type, samples).
    """
    conn = get_mysql_conn()
    tasks = []

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
            columns = cur.fetchall()

            for row in columns:
                table = row["TABLE_NAME"]
                column = row["COLUMN_NAME"]
                data_type = row["DATA_TYPE"]

                if should_skip_column(column, data_type):
                    continue

                # Use backticks for safety, even though names are from information_schema
                safe_table = table.replace("`", "``")
                safe_column = column.replace("`", "``")

                sql = f"""
                SELECT DISTINCT `{safe_column}` AS value
                FROM `{safe_table}`
                WHERE `{safe_column}` IS NOT NULL
                ORDER BY `{safe_column}`
                LIMIT 5
                """

                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                    samples = [r["value"] for r in rows]

                    if samples:
                        tasks.append((table, column, data_type, samples))
                except Exception as e:
                    print(f"[SQL Skip] {table}.{column}: {e}")

        return tasks

    finally:
        conn.close()


async def main():
    """
    The main entry point for the script.
    Orchestrates the entire profiling process from loading columns to writing the final YAML file.
    """
    print(f"🚀 Starting profiling for: {MYSQL_DB} @ {MYSQL_HOST}")

    tasks = load_candidate_columns()
    print(f"📋 Number of columns to process: {len(tasks)}")

    result: dict[str, dict[str, str]] = {"tables": {}}

    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Use tqdm for a progress bar
        for table, column, data_type, samples in tqdm(tasks, desc="AI Analyzing"):
            if table not in result["tables"]:
                result["tables"][table] = {}

            # First, try to generate a comment with cheap heuristics
            comment = heuristic_comment(table, column, data_type, samples)

            # If heuristics fail, fall back to the LLM
            if not comment:
                comment = await ask_llm_for_comment(client, table, column, data_type, samples)

            if comment:
                result["tables"][table][column] = comment

    # Write the final dictionary to a YAML file
    safe_dump_yaml(result, "dictionary.yaml")
    print("\\n✅ Profiling complete. Please check dictionary.yaml")


if __name__ == "__main__":
    asyncio.run(main())
