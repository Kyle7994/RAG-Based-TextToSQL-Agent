from fastapi import APIRouter, HTTPException
from app.models.schemas import QueryRequest
from app.services.llm_service import generate_sql_from_question
from app.services.guard_service import validate_sql
from app.services.mysql_service import run_query
from app.services.retrieval_service import retrieve_schema

router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}

@router.post("/query/debug")
async def query_debug(req: QueryRequest):
    sql = await generate_sql_from_question(req.question)
    checked_sql = validate_sql(sql)
    return {
        "question": req.question,
        "generated_sql": sql,
        "validated_sql": checked_sql,
    }

@router.post("/query/run")
async def query_run(req: QueryRequest):
    sql = await generate_sql_from_question(req.question)

    try:
        checked_sql = validate_sql(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    columns, rows = run_query(checked_sql)
    return {
        "question": req.question,
        "sql": checked_sql,
        "columns": columns,
        "rows": rows,
    }


@router.post("/debug/schema")
async def debug_schema(req: QueryRequest):
    schemas = await retrieve_schema(req.question)

    return {
        "question": req.question,
        "retrieved_schema": schemas,
    }