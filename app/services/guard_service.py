# guard_service.py

import sqlglot
from sqlglot import exp

from app.config import ENABLE_ADMIN_OPS

ALLOWED_TABLES = {"users", "orders", "products", "order_items"}


def _parse_single_statement(sql: str) -> exp.Expression:
    statements = sqlglot.parse(sql, read="mysql")
    if len(statements) != 1:
        raise ValueError("Only a single SQL statement is allowed.")
    return statements[0]


def _extract_schema_map(schema_context: str) -> dict[str, set[str]]:
    schema_map: dict[str, set[str]] = {}
    current_table: str | None = None

    for raw_line in schema_context.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if line.lower().startswith("table:"):
            current_table = line.split(":", 1)[1].strip().lower()
            schema_map[current_table] = set()
            continue

        if line.lower().startswith("columns:") and current_table:
            cols_part = line.split(":", 1)[1].strip()
            for item in cols_part.split(","):
                col_name = item.strip().split(" ", 1)[0].strip().lower()
                if col_name:
                    schema_map[current_table].add(col_name)

    return schema_map


def validate_sql(sql: str) -> str:
    try:
        parsed = _parse_single_statement(sql)
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {e}")

    tables = {t.name.lower() for t in parsed.find_all(exp.Table) if t.name}

    if not tables:
        raise ValueError("SQL must reference at least one table.")

    if not tables.issubset(ALLOWED_TABLES):
        raise ValueError(f"Forbidden tables detected: {tables - ALLOWED_TABLES}")

    if isinstance(parsed, exp.Query):
        if parsed.args.get("limit") is None:
            parsed = parsed.limit(100)
        return parsed.sql(dialect="mysql")

    if isinstance(parsed, exp.Insert):
        if not ENABLE_ADMIN_OPS:
            raise ValueError("Admin operations (INSERT) are currently disabled.")
        return parsed.sql(dialect="mysql")

    if isinstance(parsed, (exp.Update, exp.Delete)):
        if not ENABLE_ADMIN_OPS:
            raise ValueError(f"Admin operations ({parsed.key.upper()}) are currently disabled.")
        if parsed.args.get("where") is None:
            raise ValueError(f"{parsed.key.upper()} without WHERE is not allowed.")
        return parsed.sql(dialect="mysql")

    raise ValueError(f"Unsupported or highly dangerous SQL operation: {parsed.key.upper()}")


def semantic_guard(question: str, sql: str, schema_context: str) -> tuple[bool, str | None]:
    del question  # 当前版本先做 deterministic schema-grounding，不做问句语义推理

    if not schema_context or not schema_context.strip():
        return False, "Schema context is empty."

    schema_map = _extract_schema_map(schema_context)
    if not schema_map:
        return False, "Schema context could not be parsed."

    try:
        parsed = _parse_single_statement(sql)
    except Exception as e:
        return False, f"Failed to parse SQL in semantic guard: {e}"

    alias_to_table: dict[str, str] = {}
    referenced_tables: set[str] = set()

    for table in parsed.find_all(exp.Table):
        if not table.name:
            continue
        real_name = table.name.lower()
        referenced_tables.add(real_name)

        alias_name = table.alias_or_name.lower() if table.alias_or_name else real_name
        alias_to_table[alias_name] = real_name

    unknown_tables = referenced_tables - set(schema_map.keys())
    if unknown_tables:
        return False, f"SQL references tables not present in schema context: {sorted(unknown_tables)}"

    for column in parsed.find_all(exp.Column):
        column_name = column.name.lower() if column.name else None
        if not column_name or column_name == "*":
            continue

        qualifier = column.table.lower() if column.table else None
        if qualifier:
            real_table = alias_to_table.get(qualifier, qualifier)
            allowed_cols = schema_map.get(real_table)
            if allowed_cols is None:
                return False, f"Column '{column.sql()}' references unknown table/alias '{qualifier}'."
            if column_name not in allowed_cols:
                return False, f"Column '{column.sql()}' is not present in table '{real_table}'."
        else:
            search_tables = referenced_tables or set(schema_map.keys())
            if not any(column_name in schema_map.get(t, set()) for t in search_tables):
                return False, f"Unqualified column '{column_name}' was not found in referenced tables."

    return True, None