# -*- coding: utf-8 -*-

"""
app/api/routes.py

This module defines the API endpoints for the Text-to-SQL agent.

It includes routes for:
- Health checks to confirm the service is running.
- The primary text-to-SQL query execution flow.
- A debug endpoint to inspect the intermediate steps of SQL generation.
- System-level operations like schema synchronization and adding few-shot examples.

All business logic is delegated to services to keep the routing layer clean and focused.
"""

from fastapi import APIRouter
from pydantic import BaseModel

from app.models.schemas import QueryRequest
from app.services.embedding_service import get_embedding
from app.services.guard_service import semantic_guard, validate_sql
from app.services.llm_service import build_generation_context, generate_sql_from_question, repair_sql
from app.services.mysql_service import run_query
from app.services.postgres_service import save_sql_example
from app.services.redis_service import (
    bump_examples_version,
    get_cached_response,
    get_current_schema_version,
    set_cached_rejection,
    set_cached_success,
    should_cache_rejection,
    should_cache_success,
)
from app.services.schema_service import sync_mysql_schema_to_pg


class ExampleRequest(BaseModel):
    """
    Defines the request model for adding a new few-shot example.
    It requires both the natural language question and its corresponding correct SQL query.
    """
    question: str
    sql: str


router = APIRouter()

def _debug_payload(
    schema_context: str | None = None,
    examples_context: str | None = None,
    semantic_guard_passed: bool | None = None,
    semantic_guard_error: str | None = None,
) -> dict:
    return {
        "schema_context": schema_context,
        "examples_context": examples_context,
        "semantic_guard_passed": semantic_guard_passed,
        "semantic_guard_error": semantic_guard_error,
    }

@router.get("/health")
def health():
    """
    Health check endpoint.

    Returns:
        dict: A dictionary with the status "ok" if the service is running.
    """
    return {"status": "ok"}

@router.post("/query/debug")
async def query_debug(req: QueryRequest):
    schema_version = get_current_schema_version()
    if not schema_version:
        return {
            "question": req.question,
            "query_plan": None,
            "generated_sql": None,
            "validated_sql": None,
            "uncertainty_note": None,
            "answerable": False,
            "schema_version": None,
            "error": "Schema version not initialized. Please run /system/sync-schema first.",
            "is_cached": False,
            "debug": _debug_payload(),
        }

    cached = get_cached_response(req.question, schema_version=schema_version)
    if cached:
        checked_sql = None
        semantic_guard_passed = None
        semantic_guard_error = None

        if cached.get("sql"):
            try:
                checked_sql = validate_sql(cached["sql"])
            except Exception as e:
                semantic_guard_error = f"Cached SQL validation failed: {str(e)}"

        return {
            "question": req.question,
            "query_plan": cached.get("query_plan"),
            "generated_sql": cached.get("sql"),
            "validated_sql": checked_sql,
            "uncertainty_note": cached.get("uncertainty_note"),
            "answerable": cached.get("answerable", True),
            "schema_version": schema_version,
            "cache_status": cached.get("status", "unknown"),
            "error": cached.get("error"),
            "is_cached": True,
            "debug": _debug_payload(
                schema_context=None,
                examples_context=None,
                semantic_guard_passed=semantic_guard_passed,
                semantic_guard_error=semantic_guard_error,
            ),
        }

    schema_context, examples_context = await build_generation_context(req.question)

    query_plan, sql, uncertainty, answerable = await generate_sql_from_question(
        req.question,
        schema_context=schema_context,
        examples_context=examples_context,
    )

    checked_sql = None
    error_msg = None
    semantic_guard_passed = False
    semantic_guard_error = None

    if not answerable or not sql:
        rejection_reason = uncertainty or "Question cannot be answered from current schema."

        if should_cache_rejection(
            is_cached=False,
            answerable=False,
            rejection_reason=rejection_reason,
        ):
            set_cached_rejection(
                question=req.question,
                schema_version=schema_version,
                query_plan=query_plan,
                reason=rejection_reason,
                uncertainty_note=uncertainty,
            )

        return {
            "question": req.question,
            "query_plan": query_plan,
            "generated_sql": None,
            "validated_sql": None,
            "uncertainty_note": uncertainty,
            "answerable": False,
            "schema_version": schema_version,
            "cache_status": "rejected",
            "error": rejection_reason,
            "is_cached": False,
            "debug": _debug_payload(
                schema_context=schema_context,
                examples_context=examples_context,
                semantic_guard_passed=False,
                semantic_guard_error="Model returned answerable=false or empty SQL.",
            ),
        }

    try:
        checked_sql = validate_sql(sql)
    except Exception as e:
        error_msg = f"SQL validation failed: {str(e)}"
        semantic_guard_error = error_msg

    if checked_sql and not error_msg:
        semantic_guard_passed, semantic_guard_error = semantic_guard(
            question=req.question,
            sql=checked_sql,
            schema_context=schema_context,
        )

    if checked_sql and not error_msg:
        set_cached_success(
            question=req.question,
            schema_version=schema_version,
            query_plan=query_plan,
            sql=checked_sql,
            columns=[],
            rows=[],
            uncertainty_note=uncertainty,
        )
        cache_status = "success"
    else:
        cache_status = "not_cached"

    return {
        "question": req.question,
        "query_plan": query_plan,
        "generated_sql": sql,
        "validated_sql": checked_sql,
        "uncertainty_note": uncertainty,
        "answerable": answerable,
        "schema_version": schema_version,
        "cache_status": cache_status,
        "error": error_msg,
        "is_cached": False,
        "debug": _debug_payload(
            schema_context=schema_context,
            examples_context=examples_context,
            semantic_guard_passed=semantic_guard_passed,
            semantic_guard_error=semantic_guard_error,
        ),
    }
# @router.post("/query/debug")
# async def query_debug(req: QueryRequest):
#     """
#     Provides a step-by-step debug view of the Text-to-SQL generation process.

#     This endpoint simulates the query generation without executing the final SQL.
#     It returns intermediate data like the query plan, generated SQL, and cache status.
#     This is useful for troubleshooting and understanding the AI's reasoning.

#     Args:
#         req (QueryRequest): The request containing the natural language question.

#     Returns:
#         dict: A detailed breakdown of the generation process.
#     """
#     schema_version = get_current_schema_version()
#     if not schema_version:
#         return {
#             "question": req.question,
#             "query_plan": None,
#             "generated_sql": None,
#             "validated_sql": None,
#             "uncertainty_note": None,
#             "answerable": False,
#             "schema_version": None,
#             "error": "Schema version not initialized. Please run /system/sync-schema first.",
#             "is_cached": False,
#         }

#     # Check cache first for a quick response
#     cached = get_cached_response(req.question, schema_version=schema_version)
#     if cached:
#         checked_sql = None
#         if cached.get("sql"):
#             try:
#                 checked_sql = validate_sql(cached["sql"])
#             except Exception:
#                 checked_sql = None

#         return {
#             "question": req.question,
#             "query_plan": cached.get("query_plan"),
#             "generated_sql": cached.get("sql"),
#             "validated_sql": checked_sql,
#             "uncertainty_note": cached.get("uncertainty_note"),
#             "answerable": cached.get("answerable", True),
#             "schema_version": schema_version,
#             "cache_status": cached.get("status", "unknown"),
#             "error": cached.get("error"),
#             "is_cached": True,
#     }

#     # If not cached, proceed with the generation process
#     schema_context, examples_context = await build_generation_context(req.question)

#     query_plan, sql, uncertainty, answerable = await generate_sql_from_question(
#         req.question,
#         schema_context=schema_context,
#         examples_context=examples_context,
#     )

#     checked_sql = None
#     error_msg = None

#     # Handle cases where the question is deemed unanswerable
#     if not answerable or not sql:
#         rejection_reason = uncertainty or "Question cannot be answered from current schema."

#         if should_cache_rejection(
#             is_cached=False,
#             answerable=False,
#             rejection_reason=rejection_reason,
#         ):
#             set_cached_rejection(
#                 question=req.question,
#                 schema_version=schema_version,
#                 query_plan=query_plan,
#                 reason=rejection_reason,
#                 uncertainty_note=uncertainty,
#             )

#         return {
#             "question": req.question,
#             "query_plan": query_plan,
#             "generated_sql": None,
#             "validated_sql": None,
#             "uncertainty_note": uncertainty,
#             "answerable": False,
#             "schema_version": schema_version,
#             "cache_status": "rejected",
#             "error": rejection_reason,
#             "is_cached": False,
#         }

#     # Validate the generated SQL syntax
#     try:
#         checked_sql = validate_sql(sql)
#     except Exception as e:
#         error_msg = f"SQL validation failed: {str(e)}"

#     # Cache the successful generation result for future use
#     if checked_sql and not error_msg:
#         set_cached_success(
#             question=req.question,
#             schema_version=schema_version,
#             query_plan=query_plan,
#             sql=checked_sql,
#             columns=[],
#             rows=[],
#             uncertainty_note=uncertainty,
#         )
#         cache_status = "success"
#     else:
#         cache_status = "not_cached"

#     return {
#         "question": req.question,
#         "query_plan": query_plan,
#         "generated_sql": sql,
#         "validated_sql": checked_sql,
#         "uncertainty_note": uncertainty,
#         "answerable": answerable,
#         "schema_version": schema_version,
#         "cache_status": cache_status,
#         "error": error_msg,
#         "is_cached": False,
#     }


@router.post("/query/run")
async def query_run(req: QueryRequest):
    """
    The main endpoint to convert a natural language question into a SQL query and execute it.

    This function orchestrates the entire Text-to-SQL pipeline:
    1. Checks for a cached response.
    2. Builds a generation context (schema + examples).
    3. Generates the SQL using an LLM.
    4. Applies semantic and safety guards.
    5. Executes the query against the database.
    6. Attempts to self-correct/repair the SQL if execution fails.
    7. Caches the final result (success or failure).

    Args:
        req (QueryRequest): The request containing the natural language question.

    Returns:
        dict: The final result, including the executed SQL, column headers, and data rows.
    """
    uncertainty = None
    error_msg = None
    columns, rows = [], []
    checked_sql = None
    semantic_guard_passed = False
    is_cached = False

    # 1. Ensure schema is initialized before proceeding.
    schema_version = get_current_schema_version()
    if not schema_version:
        return {
            "question": req.question,
            "query_plan": None,
            "sql": None,
            "uncertainty_note": None,
            "columns": [],
            "rows": [],
            "error": "Schema version not initialized. Please run /system/sync-schema first.",
            "is_cached": False,
        }

    # 2. Check for a cached response to avoid redundant processing.
    cached = get_cached_response(req.question, schema_version=schema_version)
    if cached:
        return {
            "question": req.question,
            "query_plan": cached.get("query_plan"),
            "sql": cached.get("sql"),
            "uncertainty_note": cached.get("uncertainty_note"),
            "columns": cached.get("columns", []),
            "rows": cached.get("rows", []),
            "error": cached.get("error"),
            "cache_status": cached.get("status", "unknown"),
            "is_cached": True,
        }

    # 3. Build the context required for the LLM to generate SQL.
    schema_context, examples_context = await build_generation_context(req.question)
    if not schema_context:
        return {
            "question": req.question,
            "query_plan": None,
            "sql": None,
            "uncertainty_note": None,
            "columns": [],
            "rows": [],
            "error": "No relevant schema context found. Please run /system/sync-schema and retry.",
            "is_cached": False,
        }

    # 4. Generate the SQL query or receive a rejection if the question is unanswerable.
    query_plan, sql, uncertainty, answerable = await generate_sql_from_question(
        req.question,
        schema_context=schema_context,
        examples_context=examples_context,
    )

    if not answerable or not sql:
        rejection_reason = uncertainty or "Question cannot be answered from current schema."

        if should_cache_rejection(
            is_cached=is_cached,
            answerable=False,
            rejection_reason=rejection_reason,
        ):
            set_cached_rejection(
                question=req.question,
                schema_version=schema_version,
                query_plan=query_plan,
                reason=rejection_reason,
                uncertainty_note=uncertainty,
            )

        return {
            "question": req.question,
            "query_plan": query_plan,
            "sql": None,
            "uncertainty_note": uncertainty,
            "columns": [],
            "rows": [],
            "error": rejection_reason,
            "is_cached": False,
        }

    # 5. Apply a semantic guard to check if the generated SQL is logically sound.
    semantic_guard_passed, semantic_reason = semantic_guard(
        question=req.question,
        sql=sql,
        schema_context=schema_context,
    )

    if not semantic_guard_passed:
        rejection_reason = f"Semantic validation failed: {semantic_reason}"

        set_cached_rejection(
            question=req.question,
            schema_version=schema_version,
            query_plan=query_plan,
            reason=rejection_reason,
            uncertainty_note=uncertainty,
        )

        return {
            "question": req.question,
            "query_plan": query_plan,
            "sql": None,
            "uncertainty_note": semantic_reason,
            "columns": [],
            "rows": [],
            "error": rejection_reason,
            "is_cached": False,
        }

    # Mark that the initial semantic guard passed.
    semantic_guard_passed = True

    # 6. Execute the SQL. If it fails, attempt to repair it once.
    try:
        checked_sql = validate_sql(sql)
        columns, rows = run_query(checked_sql)

    except Exception as e:
        first_error = str(e)

        try:
            # Attempt to repair the failed SQL using the LLM.
            query_plan, repaired_sql, repaired_uncertainty = await repair_sql(
                req.question,
                first_error,
                sql,
                schema_context,
            )

            if not repaired_sql:
                raise ValueError(repaired_uncertainty or "AI could not generate a valid fix for this question.")

            # Re-run semantic guard on the repaired SQL.
            repaired_guard_passed, repaired_reason = semantic_guard(
                question=req.question,
                sql=repaired_sql,
                schema_context=schema_context,
            )
            if not repaired_guard_passed:
                raise ValueError(f"Repaired SQL failed semantic validation: {repaired_reason}")

            # Execute the repaired SQL.
            checked_sql = validate_sql(repaired_sql)
            columns, rows = run_query(checked_sql)
            uncertainty = repaired_uncertainty
            semantic_guard_passed = True

        except Exception as e2:
            # If repair fails, finalize the error.
            checked_sql = None
            error_msg = f"Self-correction failed: {str(e2)}"
            columns, rows = [], []

    # 7. Cache the final successful result.
    if should_cache_success(
        error_msg=error_msg,
        is_cached=is_cached,
        answerable=True,
        checked_sql=checked_sql,
        semantic_guard_passed=semantic_guard_passed,
    ):
        set_cached_success(
            question=req.question,
            schema_version=schema_version,
            query_plan=query_plan,
            sql=checked_sql,
            columns=columns,
            rows=rows,
            uncertainty_note=uncertainty,
        )

    return {
        "question": req.question,
        "query_plan": query_plan,
        "sql": checked_sql,
        "uncertainty_note": uncertainty,
        "columns": columns,
        "rows": rows,
        "error": error_msg,
        "is_cached": False,
    }


@router.post("/system/sync-schema")
async def api_sync_schema():
    """
    Triggers the schema synchronization process.

    This endpoint reads the schema from the source MySQL database, profiles it,
    and saves the structured information into the PostgreSQL vector database.
    This is a crucial step for the RAG system to have up-to-date context.

    Returns:
        dict: The result of the synchronization process.
    """
    result = await sync_mysql_schema_to_pg()
    return result


@router.post("/system/add-example")
async def add_example(req: ExampleRequest):
    """
    Adds a new question-SQL pair as a few-shot example.

    These examples are used to improve the accuracy of the LLM by providing
    high-quality, relevant samples during the prompt construction.

    Args:
        req (ExampleRequest): The request containing the question and its correct SQL.

    Returns:
        dict: A success message and the new version of the examples set.
    """
    checked_sql = validate_sql(req.sql)
    embedding = await get_embedding(req.question)
    save_sql_example(req.question, checked_sql, embedding)
    new_examples_version = bump_examples_version()

    return {
        "status": "success",
        "msg": "Example successfully added to knowledge base.",
        "examples_version": new_examples_version,
    }
