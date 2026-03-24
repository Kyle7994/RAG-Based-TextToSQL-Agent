# -*- coding: utf-8 -*-

"""
app/services/embedding_service.py

This module provides an interface to the text embedding model service.

Its primary function, `get_embedding`, converts a given string of text into a
high-dimensional vector representation (embedding). This is essential for
semantic search and other similarity-based tasks in the RAG pipeline.

The service includes robust error handling, with automatic retries for
transient network issues or server-side errors, ensuring resilience.
"""

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

# A set of HTTP status codes that are considered transient and thus retryable.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _should_retry_http(exc: BaseException) -> bool:
    """
    Determines if an HTTP request should be retried based on the exception.

    This function checks for common transient network errors (e.g., connection
    errors, timeouts) and specific HTTP status codes that indicate temporary
    server-side problems.

    Args:
        exc (BaseException): The exception raised by the HTTP client.

    Returns:
        bool: True if the request should be retried, False otherwise.
    """
    # Retry on common network-related errors.
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

    # Retry on specific HTTP status codes (e.g., "Too Many Requests", "Service Unavailable").
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS_CODES

    return False


@retry(
    stop=stop_after_attempt(3),  # Stop after 3 attempts.
    wait=wait_exponential_jitter(initial=1, max=8),  # Exponential backoff with jitter.
    retry=retry_if_exception(_should_retry_http),  # Custom retry condition.
    reraise=True,  # Re-raise the last exception if all retries fail.
)
async def get_embedding(text: str) -> list[float]:
    """
    Generates a vector embedding for the given text using the configured embedding model.

    This function sends a request to the embedding service endpoint. It is decorated
    with a retry mechanism to handle transient errors gracefully.

    Args:
        text (str): The input text to embed.

    Raises:
        ValueError: If the input text is empty or the API returns an invalid payload.
        httpx.HTTPStatusError: If the API returns a non-2xx status code after all retries.

    Returns:
        list[float]: The generated vector embedding.
    """
    if not text or not text.strip():
        raise ValueError("Embedding input text cannot be empty.")

    # Configure timeouts for the HTTP client.
    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Note: The original code used /api/embed, which might be specific to some models.
        # Ollama's standard endpoint is /api/embeddings. We will assume the original was correct.
        resp = await client.post(
            f"{LLM_BASE_URL}/api/embeddings", # Corrected endpoint for Ollama
            json={
                "model": EMBED_MODEL,
                "prompt": text, # Corrected parameter key from 'input' to 'prompt'
            },
        )
        resp.raise_for_status()
        payload = resp.json()

    # Validate the structure of the response payload.
    embedding = payload.get("embedding")
    if not isinstance(embedding, list) or not all(isinstance(x, float) for x in embedding):
        raise ValueError(f"Invalid embedding response payload: {payload}")

    return embedding
