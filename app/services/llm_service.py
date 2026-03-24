# llm_service.py

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

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

BASE_PROMPT = """
You are an expert MySQL SQL generator.

You MUST return exactly one JSON object with this schema:
{
  "query_plan": "Step 1: ..., Step 2: ...",
  "sql": "SELECT ...",
  "uncertainty_note": "Optional note or null",
  "answerable": true,
  "refusal_reason": null
}

Rules:
1. If answerable=true:
   - "sql" MUST be a single executable MySQL SELECT statement.
   - Do not return markdown, comments, or explanations outside the JSON object.
2. If answerable=false:
   - "sql" MUST be null.
   - "refusal_reason" MUST explain why the question cannot be answered safely.
3. Prefer answerable=false over guessing.
4. Refuse when:
   - the question refers to missing tables or columns
   - the question depends on business definitions not present in schema
   - subjective words are undefined, such as: recent, top, active, valuable, best, important
   - a safe SQL answer would require inventing joins, filters, or metrics
5. Never invent tables, columns, or business semantics.
6. Use only the tables and columns present in Context Schema.
7. uncertainty_note:
   - set to null when the question is fully grounded by schema
   - otherwise explain the ambiguity briefly
"""

REPAIR_PROMPT = """
You are an expert MySQL SQL repair assistant.

You MUST return exactly one JSON object with this schema:
{
  "query_plan": "Step 1: ..., Step 2: ...",
  "sql": "SELECT ...",
  "uncertainty_note": "Optional note or null",
  "answerable": true,
  "refusal_reason": null
}

Rules:
1. Fix the SQL only if the user question is answerable from the schema.
2. If the question is not answerable, return answerable=false and sql=null.
3. Never invent tables, columns, or business definitions.
4. Return JSON only.
"""


def _should_retry_http(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.RemoteProtocolError,
            httpx.PoolTimeout,
        ),
    ):
        return True

    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS_CODES

    return False


def _clean_json_text(raw_text: str) -> str:
    text = raw_text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def parse_llm_json_response(raw_text: str) -> dict:
    text = _clean_json_text(raw_text)
    if not text:
        raise ValueError("LLM returned empty response.")
    return json.loads(text)


def _format_examples_context(similar_examples: list[dict]) -> str:
    if not similar_examples:
        return ""

    parts = ["Here are some similar verified examples for reference:"]
    for ex in similar_examples:
        parts.append(f"Question: {ex['question']}")
        parts.append(f"SQL: {ex['sql']}")
        parts.append("")
    return "\n".join(parts).strip()


async def build_generation_context(question: str) -> tuple[str, str]:
    question_embedding = await get_embedding(question)

    relevant_schemas = search_schema_chunks(question_embedding, limit=3)
    schema_context = "\n\n".join(relevant_schemas).strip()

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
    if schema_context is None or examples_context is None:
        schema_context, examples_context = await build_generation_context(question)

    if not schema_context:
        return (
            "Step 1: Search relevant schema.\nStep 2: Refuse because no relevant schema context was found.",
            None,
            "No relevant schema context was found for this question.",
            False,
        )

    prompt = f"""{BASE_PROMPT}

Context Schema:
{schema_context}

{examples_context}

User question:
{question}
"""

    result = await _call_llm_json(prompt)

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
        uncertainty_note = uncertainty_note or refusal_reason or "Question cannot be answered from current schema."
    elif not sql:
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
    repair_prompt = f"""{REPAIR_PROMPT}

User Question:
{question}

Previous Wrong SQL:
{wrong_sql}

Execution Error:
{error_msg}

Context Schema:
{schema_context}
"""

    result = await _call_llm_json(repair_prompt)

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
        return query_plan, None, (uncertainty_note or refusal_reason or "Repair refused.")

    if not sql:
        return query_plan, None, "Repair returned answerable=true but sql was empty."

    return query_plan, sql, uncertainty_note