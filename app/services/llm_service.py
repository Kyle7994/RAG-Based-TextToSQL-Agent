# -*- coding: utf-8 -*-

"""
app/services/llm_service.py

This module is the core of the Text-to-SQL agent, handling all interactions
with the Large Language Model (LLM).

It is responsible for:
- Constructing detailed prompts for the LLM, including schema context and few-shot examples.
- Calling the LLM API to generate SQL queries from natural language questions.
- Calling the LLM API to repair incorrect SQL queries based on execution errors.
- Parsing and validating the JSON responses from the LLM.
- Implementing resilient communication with retry logic for transient errors.
"""

import json
import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from app.config import (
    HTTP_CONNECT_TIMEOUT,
    HTTP_POOL_TIMEOUT,
    HTTP_READ_TIMEOUT,
    HTTP_WRITE_TIMEOUT,
    LLM_BASE_URL,
    LLM_MODEL,
)
from app.services.embedding_service import get_embedding
from app.services.postgres_service import search_schema_chunks, search_sql_examples

# A set of HTTP status codes that are considered transient and thus retryable.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# The base prompt template for generating SQL.
# It provides the LLM with instructions, rules, and the expected JSON output format.
BASE_PROMPT = """
You are an expert MySQL SQL generator.

You MUST return exactly one JSON object with this schema:
{
  "query_plan": "A step-by-step plan for how you will construct the query.",
  "sql": "The generated SQL query, or null if not answerable.",
  "uncertainty_note": "A brief note about any ambiguities or assumptions made, or null.",
  "answerable": true,
  "refusal_reason": "The reason for refusal if the question is not answerable, or null."
}

Rules:
1. If answerable=true:
   - "sql" MUST be a single, executable MySQL SELECT statement.
   - Do not return markdown, comments, or explanations outside the JSON object.
2. If answerable=false:
   - "sql" MUST be null.
   - "refusal_reason" MUST explain why the question cannot be answered safely.
3. Prefer answerable=false over guessing.
4. Refuse when:
   - The question refers to tables or columns not present in the provided schema.
   - The question depends on business definitions not present in the schema.
   - Subjective words (e.g., 'recent', 'top', 'active') are undefined.
   - A safe answer would require inventing joins, filters, or metrics.
5. Never invent tables, columns, or business logic.
6. Use only the tables and columns present in the 'Context Schema' section.
7. For the 'uncertainty_note':
   - Set to null if the question is fully grounded by the schema.
   - Otherwise, explain the ambiguity briefly.
"""

# The prompt template for repairing a failed SQL query.
# It instructs the LLM to fix a query based on the error message it produced.
REPAIR_PROMPT = """
You are an expert MySQL SQL repair assistant.

You MUST return exactly one JSON object with this schema:
{
  "query_plan": "A step-by-step plan for how you will fix the query.",
  "sql": "The corrected SQL query, or null if not repairable.",
  "uncertainty_note": "A brief note about any ambiguities or assumptions made, or null.",
  "answerable": true,
  "refusal_reason": "The reason for refusal if the query is not repairable, or null."
}

Rules:
1. Fix the SQL only if the user's question is answerable from the provided schema.
2. If the question is not answerable, return answerable=false and sql=null.
3. Never invent new tables, columns, or business logic.
4. Return only a single JSON object.
5. Focus: Use only the tables provided in the 'Context Schema' that are strictly necessary.
"""


def _should_retry_http(exc: BaseException) -> bool:
    """
    Determines if an HTTP request to the LLM should be retried based on the exception.
    """
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError, httpx.PoolTimeout)):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return False


def _clean_json_text(raw_text: str) -> str:
    """
    Strips markdown formatting (like ```json ... ```) from the LLM's raw response.
    """
    text = raw_text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def parse_llm_json_response(raw_text: str) -> dict:
    """
    Cleans and parses the LLM's text response into a Python dictionary.
    """
    text = _clean_json_text(raw_text)
    if not text:
        raise ValueError("LLM returned an empty response.")
    return json.loads(text)


def _format_examples_context(similar_examples: list[dict]) -> str:
    """
    Formats a list of few-shot examples into a string to be included in the prompt.
    """
    if not similar_examples:
        return ""
    parts = ["Here are some similar verified examples for reference:"]
    for ex in similar_examples:
        parts.append(f"Question: {ex['question']}")
        parts.append(f"SQL: {ex['sql']}")
        parts.append("")
    return "\\n".join(parts).strip()


async def build_generation_context(question: str) -> tuple[str, str]:
    """
    Constructs the full context needed for SQL generation by retrieving relevant
    schema information and few-shot examples from the vector database.

    Args:
        question (str): The user's natural language question.

    Returns:
        A tuple containing the schema context string and the examples context string.
    """
    question_embedding = await get_embedding(question)

    # Find relevant table schemas based on semantic similarity to the question.
    relevant_schemas = search_schema_chunks(question_embedding, limit=5)
    schema_context = "\\n\\n".join(relevant_schemas).strip()

    # Find similar question/SQL pairs to use as few-shot examples.
    similar_examples = search_sql_examples(question_embedding, limit=2)
    examples_context = _format_examples_context(similar_examples)

    return schema_context, examples_context


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=8),
    retry=retry_if_exception(_should_retry_http),
    reraise=True,
)
async def _call_llm_json(prompt: str) -> dict:
    """
    A generic, retry-enabled function to call the LLM API and get a JSON response.
    """
    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/generate",
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
            },
        )
        resp.raise_for_status()
        payload = resp.json()

    raw_text = payload.get("response", "")
    return parse_llm_json_response(raw_text)


async def generate_sql_from_question(
    question: str,
    schema_context: str | None = None,
    examples_context: str | None = None,
) -> tuple[str, str | None, str | None, bool]:
    """
    Orchestrates the main Text-to-SQL generation process.

    Args:
        question (str): The user's natural language question.
        schema_context (str, optional): Pre-fetched schema context.
        examples_context (str, optional): Pre-fetched examples context.

    Returns:
        A tuple containing: (query_plan, sql, uncertainty_note, answerable).
    """
    if schema_context is None or examples_context is None:
        schema_context, examples_context = await build_generation_context(question)

    if not schema_context:
        return (
            "Step 1: Search for relevant schema.\\nStep 2: Refuse because no relevant schema was found.",
            None,
            "No relevant schema context was found for this question.",
            False,
        )

    # Assemble the final prompt.
    prompt = f"""{BASE_PROMPT}

Context Schema:
{schema_context}

{examples_context}

User question:
{question}
"""

    result = await _call_llm_json(prompt)

    # Safely extract data from the LLM's JSON response.
    query_plan = result.get("query_plan") or "No plan generated."
    answerable = bool(result.get("answerable", False))
    refusal_reason = result.get("refusal_reason")
    uncertainty_note = result.get("uncertainty_note")
    sql = result.get("sql")

    if isinstance(sql, str):
        sql = sql.strip()
    else:
        sql = None

    if not answerable:
        sql = None
        uncertainty_note = uncertainty_note or refusal_reason or "Question cannot be answered from the current schema."
    elif not sql:
        # Handle the case where the model claims it's answerable but provides no SQL.
        return (
            query_plan,
            None,
            "Model returned answerable=true but did not provide SQL.",
            False,
        )

    return query_plan, sql, uncertainty_note, answerable


async def repair_sql(
    question: str,
    error_msg: str,
    wrong_sql: str,
    schema_context: str,
) -> tuple[str, str | None, str | None]:
    """
    Attempts to repair a failed SQL query using the LLM.

    It provides the LLM with the original question, the incorrect SQL, the
    resulting error message, and the schema context.

    Args:
        question (str): The original user question.
        error_msg (str): The error message from the database.
        wrong_sql (str): The SQL query that failed.
        schema_context (str): The schema context used for the original generation.

    Returns:
        A tuple containing: (query_plan, repaired_sql, uncertainty_note).
    """
    # Assemble the repair prompt.
    prompt = f"""{REPAIR_PROMPT}

User Question:
{question}

Previous Wrong SQL:
{wrong_sql}

Execution Error:
{error_msg}

Context Schema:
{schema_context}
"""

    result = await _call_llm_json(prompt)

    query_plan = result.get("query_plan") or "No repair plan generated."
    answerable = bool(result.get("answerable", False))
    refusal_reason = result.get("refusal_reason")
    uncertainty_note = result.get("uncertainty_note")
    sql = result.get("sql")

    if isinstance(sql, str):
        sql = sql.strip()
    else:
        sql = None

    if not answerable:
        return query_plan, None, (uncertainty_note or refusal_reason or "Repair was refused.")

    if not sql:
        return query_plan, None, "Repair returned answerable=true but the SQL was empty."

    return query_plan, sql, uncertainty_note
