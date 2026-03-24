# embedding_service.py

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from app.config import (
    EMBED_MODEL,
    HTTP_CONNECT_TIMEOUT,
    HTTP_POOL_TIMEOUT,
    HTTP_READ_TIMEOUT,
    HTTP_WRITE_TIMEOUT,
    LLM_BASE_URL,
)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=8),
    retry=retry_if_exception(_should_retry_http),
    reraise=True,
)
async def get_embedding(text: str) -> list[float]:
    if not text or not text.strip():
        raise ValueError("Embedding input text cannot be empty.")

    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{LLM_BASE_URL}/api/embed",
            json={
                "model": EMBED_MODEL,
                "input": text,
            },
        )
        resp.raise_for_status()
        payload = resp.json()

    embeddings = payload.get("embeddings")
    if not isinstance(embeddings, list) or not embeddings or not isinstance(embeddings[0], list):
        raise ValueError(f"Invalid embedding response payload: {payload}")

    return embeddings[0]