from fastapi import APIRouter
from app.models.schemas import QueryRequest
from app.services.llm_service import generate_sql_from_question
from app.services.guard_service import validate_sql
from app.services.mysql_service import run_query
from app.services.schema_service import sync_mysql_schema_to_pg
from pydantic import BaseModel
from app.services.embedding_service import get_embedding
from app.services.postgres_service import save_sql_example

# 定义接收数据的格式
class ExampleRequest(BaseModel):
    question: str
    sql: str


router = APIRouter()

@router.get("/health")
def health():
    return {"status": "ok"}

@router.post("/query/debug")
async def query_debug(req: QueryRequest):
    # 接收两个返回值
    query_plan, sql = await generate_sql_from_question(req.question)
    checked_sql = validate_sql(sql)
    return {
        "question": req.question,
        "query_plan": query_plan,  # 把大模型的思考过程也返回给前端
        "generated_sql": sql,
        "validated_sql": checked_sql,
    }

@router.post("/query/run")
async def query_run(req: QueryRequest):
    # 接收两个返回值
    query_plan, sql = await generate_sql_from_question(req.question)
    checked_sql = validate_sql(sql)
    columns, rows = run_query(checked_sql)
    return {
        "question": req.question,
        "query_plan": query_plan,  # 方便前端展示：AI 是怎么想的
        "sql": checked_sql,
        "columns": columns,
        "rows": rows,
    }

@router.post("/system/sync-schema")
async def api_sync_schema():
    """触发 MySQL 表结构抽取，向量化后存入 Postgres"""
    result = await sync_mysql_schema_to_pg()
    return result


@router.post("/system/add-example")
async def add_example(req: ExampleRequest):
    """人工喂给大模型一条标准 SQL 范例"""
    embedding = await get_embedding(req.question)
    save_sql_example(req.question, req.sql, embedding)
    return {"status": "success", "msg": "Example successfully added to knowledge base!"}