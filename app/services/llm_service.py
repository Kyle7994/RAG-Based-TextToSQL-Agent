import httpx
from app.config import LLM_BASE_URL, LLM_MODEL
from app.services.retrieval_service import retrieve_schema

SYSTEM_PROMPT = """
You are a MySQL SQL generator.

Rules:
- Generate exactly one MySQL SELECT statement
- Only use tables and columns provided in the schema
- Prefer explicit column names
- Add LIMIT when appropriate
- Do not explain
- Do not output markdown
"""


async def generate_sql_from_question(question: str) -> str:
    # 1️⃣ 检索 schema
    schemas = await retrieve_schema(question)

    # 2️⃣ 拼接上下文
    context = "\n\n".join(schemas)

    # 3️⃣ 构造 prompt
    prompt = f"""{SYSTEM_PROMPT}

Relevant schema:
{context}

User question:
{question}
"""

    # 4️⃣ 调用 Ollama
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/generate",
            json={
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False,
            },
        )

        resp.raise_for_status()

        return resp.json()["response"].strip()