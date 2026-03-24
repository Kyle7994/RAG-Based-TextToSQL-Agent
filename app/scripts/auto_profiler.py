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

Design goals of this version:
- Keep the original script structure and output format.
- Avoid table-specific or column-specific hardcoded overrides.
- Use only general rules based on:
  - column-name patterns
  - data types
  - observed sample values
- Fall back to the LLM when heuristics are insufficient.
"""

import asyncio
import json
import re
from decimal import Decimal
from typing import Any, Optional

import httpx
import pymysql
import yaml
from tqdm import tqdm

from app.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DB,
    LLM_BASE_URL,
    LLM_MODEL,
    HTTP_CONNECT_TIMEOUT,
    HTTP_READ_TIMEOUT,
    HTTP_WRITE_TIMEOUT,
    HTTP_POOL_TIMEOUT,
)


COMMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "comment": {"type": "string"}
    },
    "required": ["comment"]
}


COMMON_ACRONYMS = {
    "id", "ids", "vip", "api", "sdk", "sku", "url", "uri",
    "ip", "uuid", "sql", "csv", "json", "xml", "http", "https",
    "ui", "ux", "otp", "ssn", "gps", "kpi"
}

CATEGORICAL_HINT_TOKENS = {
    "status", "state", "type", "category", "channel", "source", "method",
    "country", "region", "currency", "level", "role", "plan", "tier",
    "stage", "mode", "kind", "class", "segment", "group"
}


def get_mysql_conn():
    """
    Establishes and returns a new connection to the MySQL database.
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
    """
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def normalize_sample(v: Any) -> Any:
    """
    Normalizes a database sample value for processing.
    """
    if isinstance(v, Decimal):
        return str(v)
    return v


def normalize_text_value(v: Any) -> Optional[str]:
    """
    Convert a sample value to a normalized string, or None if empty.
    """
    v = normalize_sample(v)
    if v is None:
        return None

    text = str(v).strip()
    if not text:
        return None
    return text


def should_skip_column(column: str, data_type: str) -> bool:
    """
    Determines if a column should be skipped based on its name and data type.
    """
    c = column.lower()
    d = data_type.lower()

    if c in {"id", "created_at", "updated_at", "deleted_at"}:
        return True

    if c.endswith("_id"):
        return True

    if d not in {"varchar", "char", "enum", "boolean", "tinyint"}:
        return True

    return False


def split_identifier(name: str) -> list[str]:
    """
    Split snake_case / camelCase / mixed identifiers into lowercase tokens.
    """
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    return [p for p in s.lower().split("_") if p]


def humanize_token(token: str) -> str:
    """
    Format one identifier token into a display form.
    """
    t = token.strip().lower()
    if t in COMMON_ACRONYMS:
        return t.upper()
    return t


def humanize_identifier(name: str, drop_prefix: Optional[str] = None) -> str:
    """
    Convert an identifier into a readable phrase.

    Examples:
      is_vip -> VIP
      order_status -> order status
      api_key -> API key
    """
    parts = split_identifier(name)
    if drop_prefix and parts and parts[0] == drop_prefix:
        parts = parts[1:]

    if not parts:
        return name.strip().lower()

    return " ".join(humanize_token(p) for p in parts)


def singularize_table_name(table: str) -> str:
    """
    Convert a plural-ish table name into a rough singular entity name.
    """
    t = table.strip().lower()
    if t.endswith("ies") and len(t) > 3:
        return t[:-3] + "y"
    if t.endswith("s") and len(t) > 1:
        return t[:-1]
    return t


def choose_article(phrase: str) -> str:
    """
    Choose a/an for a phrase based on the first character.
    """
    if not phrase:
        return "a"
    return "an" if phrase[0].lower() in {"a", "e", "i", "o", "u"} else "a"


def sample_texts(samples: list[Any]) -> list[str]:
    """
    Return normalized non-empty string samples.
    """
    out: list[str] = []
    for x in samples:
        text = normalize_text_value(x)
        if text is not None:
            out.append(text)
    return out


def unique_preserve_order(values: list[str]) -> list[str]:
    """
    Deduplicate values while preserving order.
    """
    return list(dict.fromkeys(values))


def looks_boolean_tinyint(samples: list[Any]) -> bool:
    """
    Return True if all non-null sample values look boolean-like.
    """
    vals = []
    for x in samples:
        v = normalize_text_value(x)
        if v is None:
            continue
        vals.append(v.lower())

    if not vals:
        return False

    allowed = {"0", "1", "true", "false", "yes", "no", "y", "n"}
    return all(v in allowed for v in vals)


def looks_like_email(value: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))


def looks_like_url(value: str) -> bool:
    lowered = value.lower()
    return lowered.startswith("http://") or lowered.startswith("https://") or lowered.startswith("www.")


def looks_free_text(values: list[str]) -> bool:
    """
    Heuristic to detect note/description-like content.
    """
    if not values:
        return False

    for v in values:
        if len(v) > 40:
            return True
        if " " in v and len(v) > 20:
            return True
    return False


def format_sample_values(samples: list[Any], limit: int = 5) -> str:
    """
    Format up to `limit` distinct sample values for display.
    """
    vals = unique_preserve_order(sample_texts(samples))
    return ", ".join(vals[:limit])


def looks_categorical(column: str, data_type: str, samples: list[Any]) -> bool:
    """
    General heuristic for enum-like / category-like fields.

    Notes:
    - If MySQL type is enum, treat it as categorical.
    - Otherwise, combine column-name token hints with sample-shape checks.
    - Avoid labeling obvious free-text, email, or URL fields as categorical.
    """
    d = data_type.lower()
    if d == "enum":
        return True

    values = unique_preserve_order(sample_texts(samples))
    if not values:
        return False

    if looks_free_text(values):
        return False

    if any(looks_like_email(v) for v in values):
        return False

    if any(looks_like_url(v) for v in values):
        return False

    # With current sampling query (DISTINCT ... LIMIT 5), we cannot reliably infer
    # true low cardinality from unique count alone, so we require lexical hints too.
    tokens = set(split_identifier(column))
    has_categorical_name_hint = bool(tokens & CATEGORICAL_HINT_TOKENS)

    if not has_categorical_name_hint:
        return False

    if any(len(v) > 32 for v in values):
        return False

    return True


def clean_comment_text(text: str) -> str:
    """
    Normalize whitespace and trim wrappers.
    """
    text = re.sub(r"\s+", " ", (text or "").strip())
    text = text.strip("\"'` ")
    return text


def post_process_comment(comment: str) -> str:
    """
    Apply only generic cleanup. No field-specific rewrites.
    """
    text = clean_comment_text(comment)
    if not text:
        return ""

    replacements = {
        "Categorical field; observed values:": "Categorical field. Observed values include:",
        "Categorical field; Observed values:": "Categorical field. Observed values include:",
        "Boolean flag for ": "Boolean flag indicating ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    if text:
        text = text[0].upper() + text[1:]

    if text and not text.endswith("."):
        text += "."

    return text


def heuristic_comment(table: str, column: str, data_type: str, samples: list[Any]) -> str:
    """
    Generate a general-purpose heuristic comment.

    No table-specific overrides.
    No field-specific overrides.
    Only:
    - column name patterns
    - data type
    - observed sample values
    """
    entity = singularize_table_name(table)
    c = column.strip().lower()
    d = data_type.strip().lower()
    cleaned = [normalize_sample(x) for x in samples if x is not None]

    # Pattern-based boolean fields
    if c.startswith("is_"):
        predicate = humanize_identifier(column, drop_prefix="is")
        return f"Boolean flag indicating whether the {entity} is {predicate}."

    if c.startswith("has_"):
        obj = humanize_identifier(column, drop_prefix="has")
        article = choose_article(obj)
        return f"Boolean flag indicating whether the {entity} has {article} {obj}."

    if c.startswith("can_"):
        action = humanize_identifier(column, drop_prefix="can")
        return f"Boolean flag indicating whether the {entity} can {action}."

    # Type-based boolean fallback
    if d == "boolean" or (d == "tinyint" and looks_boolean_tinyint(cleaned)):
        attr = humanize_identifier(column)
        return f"Boolean indicator for {attr} of the {entity}."

    # Enum-like / categorical fields
    if looks_categorical(column, data_type, cleaned):
        values = format_sample_values(cleaned, limit=5)
        if values:
            return f"Categorical field. Observed values include: {values}."
        return "Categorical field."

    return ""


def extract_json_object(text: str) -> Optional[dict[str, Any]]:
    """
    Robustly extracts a JSON object from a string.
    """
    text = text.strip()

    try:
        return json.loads(text)
    except Exception:
        pass

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
    Ask the configured LLM to generate a short, general dictionary comment.
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

Write one short factual sentence in plain English.

Rules:
- Use only the column name, data type, and sample values.
- Do not infer hidden business meaning.
- Do not assume application-specific semantics.
- Prefer general wording over specific interpretation.
- If the field looks categorical, mention observed values.
- If uncertain, describe it generically but naturally.
- Aim for 6 to 18 words.
- Respond with JSON only.

Good examples:
- "Boolean flag indicating whether the record is active."
- "Categorical field. Observed values include: paid, pending, refunded."
- "Text field associated with the record."
- "Identifier-like text field associated with the record."

Bad examples:
- "Internal premium monetization eligibility flag."
- "VIP switch used by the marketing team."
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
        print(f"\n[LLM Error] {table}.{column}: {type(e).__name__}: {e}")
        return ""


def load_candidate_columns():
    """
    Connect to the database and retrieve candidate columns to profile.
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
    Main entry point for the script.
    """
    print(f"Starting profiling for: {MYSQL_DB} @ {MYSQL_HOST}")

    tasks = load_candidate_columns()
    print(f"Number of columns to process: {len(tasks)}")

    result: dict[str, dict[str, str]] = {"tables": {}}

    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        for table, column, data_type, samples in tqdm(tasks, desc="AI Analyzing"):
            if table not in result["tables"]:
                result["tables"][table] = {}

            comment = heuristic_comment(table, column, data_type, samples)

            if not comment:
                comment = await ask_llm_for_comment(client, table, column, data_type, samples)

            comment = post_process_comment(comment)

            if comment:
                result["tables"][table][column] = comment

    safe_dump_yaml(result, "dictionary.yaml")
    print("\nProfiling complete. Please check dictionary.yaml")


if __name__ == "__main__":
    asyncio.run(main())