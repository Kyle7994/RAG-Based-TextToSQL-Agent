# -*- coding: utf-8 -*-

"""
app/services/redis_service.py

This module implements the caching layer for the Text-to-SQL application using Redis.

It provides a sophisticated caching mechanism to store both successful SQL generation
results and deliberate rejections. The cache aims to:
- Reduce latency for repeated questions.
- Minimize redundant calls to the LLM, saving costs and resources.
- Ensure cache integrity through a versioned fingerprinting system.

The cache key is a hash composed of the user's question and versions of all major
system components (prompt, model, schema, etc.). This ensures that if any part
of the generation pipeline changes, the cache is automatically invalidated.
"""

import json
import hashlib
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
import redis

from app.config import REDIS_URL, LLM_MODEL

# ===================================
# Cache Configuration
# ===================================
CACHE_ENV = os.getenv("CACHE_ENV", "dev")
SUCCESS_TTL_SECONDS = int(os.getenv("SUCCESS_TTL_SECONDS", "3600"))  # 1 hour
REJECT_TTL_SECONDS = int(os.getenv("REJECT_TTL_SECONDS", "300"))    # 5 minutes

# ===================================
# Cache Versioning
#
# These versions are part of the cache key. Bumping any of these versions
# effectively invalidates the entire cache, forcing regeneration of responses.
# ===================================
CACHE_VERSION = "v3"  # Global version for the cache structure itself.
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "prompt_v4")
GUARD_VERSION = os.getenv("GUARD_VERSION", "guard_v3")
VALIDATOR_VERSION = os.getenv("VALIDATOR_VERSION", "validator_v2")
DEFAULT_EXAMPLES_VERSION = os.getenv("EXAMPLES_VERSION", "examples_v1")
MODEL_NAME = os.getenv("LLM_MODEL", LLM_MODEL or "unknown_model")

# ===================================
# Redis Keys for Dynamic Versions
# ===================================
CURRENT_SCHEMA_VERSION_KEY = "nl2sql:current_schema_version"
CURRENT_EXAMPLES_VERSION_KEY = "nl2sql:current_examples_version"

# Initialize the Redis client.
redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
    health_check_interval=30,
    retry_on_timeout=True,
)

# ===================================
# Helper Functions
# ===================================

def _make_json_safe(value: Any) -> Any:
    """Recursively converts non-JSON-serializable types (Decimal, datetime) to strings."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_json_safe(v) for v in value]
    return value


def utc_now_iso() -> str:
    """Returns the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def normalize_question(question: str) -> str:
    """
    Normalizes a question for caching by converting to lowercase, stripping whitespace,
    and collapsing multiple spaces.
    """
    return " ".join(question.strip().lower().split())

# ===================================
# Dynamic Version Management
# ===================================

def set_current_schema_version(schema_version: str) -> None:
    """Sets the current global schema version in Redis."""
    try:
        redis_client.set(CURRENT_SCHEMA_VERSION_KEY, schema_version)
    except redis.RedisError:
        # Fail silently if Redis is unavailable.
        pass


def get_current_schema_version() -> str | None:
    """Retrieves the current global schema version from Redis."""
    try:
        return redis_client.get(CURRENT_SCHEMA_VERSION_KEY)
    except redis.RedisError:
        return None


def get_current_examples_version() -> str:
    """Retrieves the current global few-shot examples version from Redis."""
    try:
        return redis_client.get(CURRENT_EXAMPLES_VERSION_KEY) or DEFAULT_EXAMPLES_VERSION
    except redis.RedisError:
        return DEFAULT_EXAMPLES_VERSION


def bump_examples_version() -> str:
    """Increments the version of the few-shot examples, effectively invalidating related caches."""
    current = get_current_examples_version()
    try:
        # Assumes version is in "examples_v<number>" format.
        suffix = int(current.rsplit("v", 1)[1])
        new_version = f"examples_v{suffix + 1}"
    except (IndexError, ValueError):
        # Fallback if the format is unexpected.
        new_version = f"{DEFAULT_EXAMPLES_VERSION}_bumped_at_{utc_now_iso()}"

    try:
        redis_client.set(CURRENT_EXAMPLES_VERSION_KEY, new_version)
    except redis.RedisError:
        pass
    return new_version


def _resolve_examples_version(examples_version: str | None = None) -> str:
    """
    A helper to use the provided examples_version or fall back to the global one.
    """
    return examples_version or get_current_examples_version()

# ===================================
# Cache Key Generation
# ===================================

def compute_fingerprint(
    question: str,
    schema_version: str,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> str:
    """
    Computes a SHA256 hash based on the question and all relevant component versions.
    This fingerprint uniquely identifies a query context.
    """
    normalized_question = normalize_question(question)
    resolved_examples_version = _resolve_examples_version(examples_version)

    # Concatenate all versioned components into a single string.
    raw = "||".join(
        [
            normalized_question,
            schema_version,
            prompt_version,
            model_name,
            guard_version,
            validator_version,
            resolved_examples_version,
            CACHE_VERSION,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_cache_key(
    question: str,
    schema_version: str,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> str:
    """Constructs the full Redis cache key from the computed fingerprint."""
    fp = compute_fingerprint(
        question=question, schema_version=schema_version,
        prompt_version=prompt_version, model_name=model_name,
        guard_version=guard_version, validator_version=validator_version,
        examples_version=examples_version,
    )
    return f"nl2sql:cache:{CACHE_ENV}:{fp}"

# ===================================
# Cache Read/Write Operations
# ===================================

def get_cached_response(question: str, schema_version: str, **kwargs) -> Optional[dict[str, Any]]:
    """
    Retrieves a cached response from Redis.

    It builds the cache key, fetches the data, and performs validation checks
    (e.g., ensuring the cache_version matches) before returning the payload.
    """
    key = build_cache_key(question=question, schema_version=schema_version, **kwargs)
    try:
        raw = redis_client.get(key)
    except redis.RedisError:
        return None

    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        # Delete corrupted data from the cache.
        try:
            redis_client.delete(key)
        except redis.RedisError:
            pass
        return None

    # Invalidate if the cache item was created with an old cache structure.
    if payload.get("cache_version") != CACHE_VERSION:
        try:
            redis_client.delete(key)
        except redis.RedisError:
            pass
        return None

    return payload


def set_cached_success(
    question: str, schema_version: str, query_plan: str, sql: str,
    columns: list[str], rows: list[list[Any]],
    uncertainty_note: str | None = None, ttl_seconds: int = SUCCESS_TTL_SECONDS,
    **kwargs
) -> None:
    """Stores a successful SQL generation result in the cache."""
    key = build_cache_key(question=question, schema_version=schema_version, **kwargs)
    payload = {
        "status": "success", "question": question, "sql": sql,
        "query_plan": query_plan, "columns": columns, "rows": rows,
        "uncertainty_note": uncertainty_note, "answerable": True, "error": None,
        "schema_version": schema_version, "cache_version": CACHE_VERSION,
        "created_at": utc_now_iso(), "ttl_seconds": ttl_seconds,
        **kwargs
    }
    try:
        safe_payload = _make_json_safe(payload)
        redis_client.setex(key, ttl_seconds, json.dumps(safe_payload, ensure_ascii=False))
    except redis.RedisError:
        pass


def set_cached_rejection(
    question: str, schema_version: str, query_plan: str, reason: str,
    uncertainty_note: str | None = None, ttl_seconds: int = REJECT_TTL_SECONDS,
    **kwargs
) -> None:
    """Stores a rejection (i.e., the question was deemed unanswerable) in the cache."""
    key = build_cache_key(question=question, schema_version=schema_version, **kwargs)
    payload = {
        "status": "rejected", "question": question, "sql": None,
        "query_plan": query_plan, "columns": [], "rows": [],
        "uncertainty_note": uncertainty_note, "answerable": False, "error": reason,
        "schema_version": schema_version, "cache_version": CACHE_VERSION,
        "created_at": utc_now_iso(), "ttl_seconds": ttl_seconds,
        **kwargs
    }
    try:
        safe_payload = _make_json_safe(payload)
        redis_client.setex(key, ttl_seconds, json.dumps(safe_payload, ensure_ascii=False))
    except redis.RedisError:
        pass

# ===================================
# Cache Decision Logic
# ===================================

def should_cache_success(
    *, error_msg: str | None, is_cached: bool, answerable: bool,
    checked_sql: str | None, semantic_guard_passed: bool
) -> bool:
    """Determines if a successful result is eligible for caching."""
    return (
        error_msg is None and not is_cached and answerable and
        checked_sql is not None and checked_sql.strip() != "" and
        semantic_guard_passed
    )


def should_cache_rejection(
    *, is_cached: bool, answerable: bool, rejection_reason: str | None
) -> bool:
    """Determines if a rejection is eligible for caching."""
    return (
        not is_cached and not answerable and
        rejection_reason is not None and rejection_reason.strip() != ""
    )
