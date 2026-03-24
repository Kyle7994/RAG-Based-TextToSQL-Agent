# -*- coding: utf-8 -*-

"""
app/config.py

This module centralizes the application's configuration settings.

It retrieves settings from environment variables with sensible defaults,
making the application easily configurable for different environments (development, testing, production).

This includes configurations for:
- MySQL database connection
- PostgreSQL database connection
- Redis connection
- Large Language Model (LLM) service
- HTTP client timeouts
- SQL safety features
"""

import os

# ===================================
# Database Configurations
# ===================================

# MySQL connection settings for the application's primary database.
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "ecommerce")
MYSQL_USER = os.getenv("MYSQL_USER", "appuser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "apppass")

# PostgreSQL connection settings, typically used for vector storage with pgvector.
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "retrieval")
PG_USER = os.getenv("PG_USER", "pguser")
PG_PASSWORD = os.getenv("PG_PASSWORD", "pgpass")

# ===================================
# Caching and In-Memory Storage
# ===================================

# Redis connection settings, used for caching and session management.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_URL = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

# ===================================
# AI and Machine Learning Models
# ===================================

# Configuration for the Large Language Model (LLM) service.
# Specifies the base URL of the LLM provider (e.g., Ollama) and the model to use.
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://ollama:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3")

# Configuration for the embedding model, used for converting text to vector embeddings.
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")

# ===================================
# Network and HTTP Settings
# ===================================

# Timeouts for outgoing HTTP requests to prevent the application from hanging.
# Connect timeout: Time to wait for the initial connection to be established.
HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "5"))
# Read timeout: Time to wait for a response after the connection is made.
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "120"))
# Write timeout: Time to wait for a write operation to complete.
HTTP_WRITE_TIMEOUT = float(os.getenv("HTTP_WRITE_TIMEOUT", "30"))
# Pool timeout: Time to wait for a connection from the connection pool.
HTTP_POOL_TIMEOUT = float(os.getenv("HTTP_POOL_TIMEOUT", "5"))

# ===================================
# Security and Safety Features
# ===================================

# A flag to enable or disable potentially destructive SQL operations (e.g., DELETE, UPDATE).
# This acts as a safety guardrail. Set to "true" to allow admin-level operations.
ENABLE_ADMIN_OPS = os.getenv("ENABLE_ADMIN_OPS", "false").lower() == "true"
