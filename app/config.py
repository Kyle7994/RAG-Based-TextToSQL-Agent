import os

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "ecommerce")
MYSQL_USER = os.getenv("MYSQL_USER", "appuser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "apppass")

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://ollama:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3")
LLM_EMBED_MODEL = os.getenv("LLM_EMBED_MODEL", "nomic-embed-text")