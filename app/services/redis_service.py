# redis_service.py

import json
import hashlib
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
import redis

from app.config import REDIS_URL, LLM_MODEL

CACHE_ENV = os.getenv("CACHE_ENV", "dev")

SUCCESS_TTL_SECONDS = int(os.getenv("SUCCESS_TTL_SECONDS", "3600"))
REJECT_TTL_SECONDS = int(os.getenv("REJECT_TTL_SECONDS", "300"))

CACHE_VERSION = "v3"
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "prompt_v4")
GUARD_VERSION = os.getenv("GUARD_VERSION", "guard_v3")
VALIDATOR_VERSION = os.getenv("VALIDATOR_VERSION", "validator_v2")
DEFAULT_EXAMPLES_VERSION = os.getenv("EXAMPLES_VERSION", "examples_v1")
MODEL_NAME = os.getenv("LLM_MODEL", LLM_MODEL or "unknown_model")

CURRENT_SCHEMA_VERSION_KEY = "nl2sql:current_schema_version"
CURRENT_EXAMPLES_VERSION_KEY = "nl2sql:current_examples_version"

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
    health_check_interval=30,
    retry_on_timeout=True,
)

def _make_json_safe(value: Any) -> Any:
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
    return datetime.now(timezone.utc).isoformat()


def normalize_question(question: str) -> str:
    return " ".join(question.strip().lower().split())


def set_current_schema_version(schema_version: str) -> None:
    try:
        redis_client.set(CURRENT_SCHEMA_VERSION_KEY, schema_version)
    except redis.RedisError:
        pass


def get_current_schema_version() -> str | None:
    try:
        return redis_client.get(CURRENT_SCHEMA_VERSION_KEY)
    except redis.RedisError:
        return None


def get_current_examples_version() -> str:
    try:
        return redis_client.get(CURRENT_EXAMPLES_VERSION_KEY) or DEFAULT_EXAMPLES_VERSION
    except redis.RedisError:
        return DEFAULT_EXAMPLES_VERSION


def bump_examples_version() -> str:
    current = get_current_examples_version()

    try:
        suffix = int(current.rsplit("v", 1)[1])
        new_version = f"examples_v{suffix + 1}"
    except Exception:
        new_version = f"{DEFAULT_EXAMPLES_VERSION}_bumped"

    try:
        redis_client.set(CURRENT_EXAMPLES_VERSION_KEY, new_version)
    except redis.RedisError:
        pass

    return new_version


def _resolve_examples_version(examples_version: str | None = None) -> str:
    return examples_version or get_current_examples_version()


def compute_fingerprint(
    question: str,
    schema_version: str,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> str:
    normalized_question = normalize_question(question)
    resolved_examples_version = _resolve_examples_version(examples_version)

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
    fp = compute_fingerprint(
        question=question,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model_name=model_name,
        guard_version=guard_version,
        validator_version=validator_version,
        examples_version=examples_version,
    )
    return f"nl2sql:cache:{CACHE_ENV}:{fp}"


def get_cached_response(
    question: str,
    schema_version: str,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> Optional[dict[str, Any]]:
    key = build_cache_key(
        question=question,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model_name=model_name,
        guard_version=guard_version,
        validator_version=validator_version,
        examples_version=examples_version,
    )

    try:
        raw = redis_client.get(key)
    except redis.RedisError:
        return None

    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        try:
            redis_client.delete(key)
        except redis.RedisError:
            pass
        return None

    if payload.get("cache_version") != CACHE_VERSION:
        try:
            redis_client.delete(key)
        except redis.RedisError:
            pass
        return None

    return payload


def set_cached_success(
    question: str,
    schema_version: str,
    query_plan: str,
    sql: str,
    columns: list[str],
    rows: list[list[Any]],
    uncertainty_note: str | None = None,
    semantic_guard_reason: str | None = None,
    ttl_seconds: int = SUCCESS_TTL_SECONDS,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> None:
    resolved_examples_version = _resolve_examples_version(examples_version)
    key = build_cache_key(
        question=question,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model_name=model_name,
        guard_version=guard_version,
        validator_version=validator_version,
        examples_version=resolved_examples_version,
    )

    payload = {
        "status": "success",
        "question": question,
        "normalized_question": normalize_question(question),
        "query_plan": query_plan,
        "sql": sql,
        "uncertainty_note": uncertainty_note,
        "answerable": True,
        "validated": True,
        "semantic_guard_passed": True,
        "semantic_guard_reason": semantic_guard_reason,
        "columns": columns,
        "rows": rows,
        "error": None,
        "schema_version": schema_version,
        "prompt_version": prompt_version,
        "model_name": model_name,
        "guard_version": guard_version,
        "validator_version": validator_version,
        "examples_version": resolved_examples_version,
        "cache_version": CACHE_VERSION,
        "created_at": utc_now_iso(),
        "ttl_seconds": ttl_seconds,
    }

    try:
        safe_payload = _make_json_safe(payload)
        redis_client.setex(key, ttl_seconds, json.dumps(safe_payload, ensure_ascii=False))
    except redis.RedisError:
        pass


def set_cached_rejection(
    question: str,
    schema_version: str,
    query_plan: str,
    reason: str,
    uncertainty_note: str | None = None,
    ttl_seconds: int = REJECT_TTL_SECONDS,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> None:
    resolved_examples_version = _resolve_examples_version(examples_version)
    key = build_cache_key(
        question=question,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model_name=model_name,
        guard_version=guard_version,
        validator_version=validator_version,
        examples_version=resolved_examples_version,
    )

    payload = {
        "status": "rejected",
        "question": question,
        "normalized_question": normalize_question(question),
        "query_plan": query_plan,
        "sql": None,
        "uncertainty_note": uncertainty_note,
        "answerable": False,
        "validated": False,
        "semantic_guard_passed": False,
        "semantic_guard_reason": reason,
        "columns": [],
        "rows": [],
        "error": reason,
        "schema_version": schema_version,
        "prompt_version": prompt_version,
        "model_name": model_name,
        "guard_version": guard_version,
        "validator_version": validator_version,
        "examples_version": resolved_examples_version,
        "cache_version": CACHE_VERSION,
        "created_at": utc_now_iso(),
        "ttl_seconds": ttl_seconds,
    }

    try:
        safe_payload = _make_json_safe(payload)
        redis_client.setex(key, ttl_seconds, json.dumps(safe_payload, ensure_ascii=False))
    except redis.RedisError:
        pass


def delete_cached_response(
    question: str,
    schema_version: str,
    prompt_version: str = PROMPT_VERSION,
    model_name: str = MODEL_NAME,
    guard_version: str = GUARD_VERSION,
    validator_version: str = VALIDATOR_VERSION,
    examples_version: str | None = None,
) -> bool:
    key = build_cache_key(
        question=question,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model_name=model_name,
        guard_version=guard_version,
        validator_version=validator_version,
        examples_version=examples_version,
    )
    try:
        return redis_client.delete(key) > 0
    except redis.RedisError:
        return False


def should_cache_success(
    *,
    error_msg: str | None,
    is_cached: bool,
    answerable: bool,
    checked_sql: str | None,
    semantic_guard_passed: bool,
) -> bool:
    return (
        error_msg is None
        and is_cached is False
        and answerable is True
        and checked_sql is not None
        and checked_sql.strip() != ""
        and semantic_guard_passed is True
    )


def should_cache_rejection(
    *,
    is_cached: bool,
    answerable: bool,
    rejection_reason: str | None,
) -> bool:
    return (
        is_cached is False
        and answerable is False
        and rejection_reason is not None
        and rejection_reason.strip() != ""
    )