# routes.py

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
    question: str
    sql: str


router = APIRouter()


@router.get("/health")
def health():
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
        }

    cached = get_cached_response(req.question, schema_version=schema_version)
    if cached:
        checked_sql = None
        if cached.get("sql"):
            try:
                checked_sql = validate_sql(cached["sql"])
            except Exception:
                checked_sql = None

        return {
            "question": req.question,
            "query_plan": cached.get("query_plan"),
            "generated_sql": cached.get("sql"),
            "validated_sql": checked_sql,
            "uncertainty_note": cached.get("uncertainty_note"),
            "answerable": cached.get("answerable", True),
            "schema_version": schema_version,
            "cache_status": "not_cached",
            "error": cached.get("error"),
            "is_cached": True,
        }

    schema_context, examples_context = await build_generation_context(req.question)

    query_plan, sql, uncertainty, answerable = await generate_sql_from_question(
        req.question,
        schema_context=schema_context,
        examples_context=examples_context,
    )

    checked_sql = None
    error_msg = None

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
        }

    try:
        checked_sql = validate_sql(sql)
    except Exception as e:
        error_msg = f"SQL validation failed: {str(e)}"

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
    }


@router.post("/query/run")
async def query_run(req: QueryRequest):
    uncertainty = None
    error_msg = None
    columns, rows = [], []
    checked_sql = None
    semantic_guard_passed = False
    is_cached = False

    # 1) 先拿全局 schema_version，没初始化就 fail-closed
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

    # 2) 先查缓存，命中则直接返回
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
            "is_cached": True,
        }

    # 3) 缓存未命中时，构造 generation context
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

    # 4) 生成 SQL / 或拒答
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

    # 5) semantic guard
    semantic_guard_passed, semantic_reason = semantic_guard(
        question=req.question,
        sql=sql,
        schema_context=schema_context,
    )

    if not semantic_guard_passed:
        semantic_guard_passed = True
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

    # 关键：首次 guard 通过就标记 True，不要只在 repair 成功后才设
    semantic_guard_passed = True

    # 6) 执行；失败时尝试一次 repair
    try:
        checked_sql = validate_sql(sql)
        columns, rows = run_query(checked_sql)

    except Exception as e:
        first_error = str(e)

        try:
            query_plan, repaired_sql, repaired_uncertainty = await repair_sql(
                req.question,
                first_error,
                sql,
                schema_context,
            )

            if not repaired_sql:
                raise ValueError(repaired_uncertainty or "AI could not generate a valid fix for this question.")

            repaired_guard_passed, repaired_reason = semantic_guard(
                question=req.question,
                sql=repaired_sql,
                schema_context=schema_context,
            )
            if not repaired_guard_passed:
                raise ValueError(f"Repaired SQL failed semantic validation: {repaired_reason}")

            checked_sql = validate_sql(repaired_sql)
            columns, rows = run_query(checked_sql)
            uncertainty = repaired_uncertainty
            semantic_guard_passed = True

        except Exception as e2:
            checked_sql = None
            error_msg = f"Self-correction failed: {str(e2)}"
            columns, rows = [], []

    # 7) 成功才缓存
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
    result = await sync_mysql_schema_to_pg()
    return result


@router.post("/system/add-example")
async def add_example(req: ExampleRequest):
    checked_sql = validate_sql(req.sql)
    embedding = await get_embedding(req.question)
    save_sql_example(req.question, checked_sql, embedding)
    new_examples_version = bump_examples_version()

    return {
        "status": "success",
        "msg": "Example successfully added to knowledge base.",
        "examples_version": new_examples_version,
    }