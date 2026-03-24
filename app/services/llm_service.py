import httpx
import json
from app.config import LLM_BASE_URL, LLM_MODEL
from app.services.embedding_service import get_embedding
from app.services.postgres_service import search_schema_chunks, search_sql_examples

# 提示词大升级：强制要求输出 JSON，并且必须先写 query_plan
BASE_PROMPT = """
You are an expert MySQL SQL generator. 

You MUST respond strictly in the following JSON format, do not include markdown blocks (like ```json), just output the raw JSON string:
{
    "query_plan": "Step 1: ..., Step 2: ..., Step 3: ...",
    "sql": "SELECT ..."
}

Rules:
- First, write a detailed step-by-step query_plan in the "query_plan" field. Explain which tables to join, what filters to apply, and how to group/aggregate.
- Then, write exactly one valid MySQL SELECT statement in the "sql" field.
- Only use existing tables and columns provided in the Context Schema below.
- Do not explain outside of the JSON structure.
"""

async def generate_sql_from_question(question: str) -> tuple[str, str]:
    question_embedding = await get_embedding(question)
    
    relevant_schemas = search_schema_chunks(question_embedding, limit=3)
    schema_context = "\n\n".join(relevant_schemas)
    
    similar_examples = search_sql_examples(question_embedding, limit=2)
    examples_context = ""
    if similar_examples:
        examples_context = "Here are some similar verified examples for reference:\n"
        for ex in similar_examples:
            examples_context += f"Question: {ex['question']}\nSQL: {ex['sql']}\n\n"
    
    prompt = f"""{BASE_PROMPT}

Context Schema:
{schema_context}

{examples_context}
User question:
{question}
"""
    print("--- DYNAMIC PROMPT ---")
    print(prompt)
    print("----------------------")

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/generate",
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json"  # Ollama 的特殊参数，强制约束模型按 JSON 格式吐字
            },
        )
        resp.raise_for_status()
        response_text = resp.json()["response"].strip()
        
        # 解析返回的 JSON
        try:
            result = json.loads(response_text)
            return result.get("query_plan", "No plan generated."), result.get("sql", "")
        except json.JSONDecodeError:
            # 兜底：万一大模型没听话，带了 markdown 壳子
            if response_text.startswith("```json"):
                response_text = response_text[7:-3].strip()
            result = json.loads(response_text)
            return result.get("query_plan", ""), result.get("sql", "")