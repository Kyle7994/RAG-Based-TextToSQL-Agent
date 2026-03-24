# embedding_service.py
import httpx
from app.config import LLM_BASE_URL

async def get_embedding(text: str):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/embeddings",
            json={
                "model": "nomic-embed-text",
                "prompt": text
            }
        )
        return resp.json()["embedding"]