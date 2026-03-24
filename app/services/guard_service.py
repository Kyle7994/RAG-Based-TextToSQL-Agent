# 指定源码文件的编码为 UTF-8，避免中文等字符出现编码问题
# -*- coding: utf-8 -*-

"""
这个文件位于 app/services/guard_service.py

整个模块的作用：
给 SQL 生成流程加一层“安全与校验”保护。

它主要提供两个核心能力：
1. validate_sql
   - 做 SQL 的语法/安全校验
   - 限制危险操作
   - 限制可访问表
   - 给 SELECT 自动补 LIMIT

2. semantic_guard
   - 做 SQL 的“语义校验”
   - 检查 SQL 里引用的表和列，是否真的存在于提供给 LLM 的 schema 上下文里
   - 防止模型幻觉出不存在的表或字段
"""

# 引入 sqlglot，用来解析 SQL
import sqlglot

# 从 sqlglot 中引入 exp（表达式 AST 节点类型）
# 后面会用 exp.Table / exp.Query / exp.Insert / exp.Update / exp.Delete 等类型判断 SQL 结构
from sqlglot import exp

# 引入 qualify，用于结合 schema 去校验 SQL 中的表/列能不能被正确解析
from sqlglot.optimizer.qualify import qualify

# 从项目配置中读取是否允许管理类操作（INSERT / UPDATE / DELETE）
from app.config import ENABLE_ADMIN_OPS

# 引入 Collection 类型，用作类型标注
# 表示 allowed_tables 这种参数只要是“可迭代集合”即可，不一定非得是 set
from collections.abc import Collection


# 这个方法的作用：
# 对外的第一层 SQL 安全检查入口。
# 它负责：
# 1. 确保输入只有一条 SQL
# 2. 检查 SQL 至少引用了一张表
# 3. 如果传了 allowed_tables，就只允许访问白名单里的表
# 4. SELECT 没有 LIMIT 时自动补 LIMIT 100
# 5. INSERT / UPDATE / DELETE 根据配置决定是否允许
# 6. UPDATE / DELETE 必须带 WHERE，防止误操作整表
def validate_sql(sql: str, allowed_tables: Collection[str] | None = None) -> str:
    """
    解析、校验并标准化一条 SQL。

    参数：
        sql:
            原始 SQL 字符串
        allowed_tables:
            允许访问的表名集合；如果传了，就会做白名单限制

    返回：
        校验通过后的 SQL 字符串
        （可能和输入不同，比如 SELECT 会自动加 LIMIT 100）

    异常：
        ValueError:
            SQL 不合法、访问了未授权表、执行了不允许的危险操作时抛出
    """

    # 先尝试把 SQL 解析成 AST，并且确保只有一条语句
    try:
        parsed = _parse_single_statement(sql)

    # 如果解析失败，统一包装成更明确的 ValueError
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {e}")

    # 从解析后的 SQL AST 中，找出所有被引用到的表名
    # 统一转小写，避免大小写差异导致判断出错
    tables = {t.name.lower() for t in parsed.find_all(exp.Table) if t.name}

    # 如果一张表都没有引用，认为这条 SQL 不符合预期
    if not tables:
        raise ValueError("SQL must reference at least one table.")

    # 如果调用方传了 allowed_tables，就做表级白名单校验
    if allowed_tables is not None:
        # 先把允许的表名也统一转小写，和 SQL 中抽取出的表名对齐
        normalized_allowed_tables = {t.lower() for t in allowed_tables}

        # 如果 SQL 引用的表，不是 allowed_tables 的子集，说明访问了不允许的表
        if not tables.issubset(normalized_allowed_tables):
            raise ValueError(f"Forbidden tables detected: {tables - normalized_allowed_tables}")

    # 如果这是一条查询类语句（如 SELECT）
    if isinstance(parsed, exp.Query):

        # 检查是否已经带 LIMIT
        if parsed.args.get("limit") is None:
            # 如果没带 LIMIT，就自动补一个 LIMIT 100
            # 目的是防止模型生成无上限查询，把全表大量数据扫出来
            parsed = parsed.limit(100)

        # 把 AST 再转回 MySQL 方言 SQL 并返回
        return parsed.sql(dialect="mysql")

    # 如果是 INSERT 语句
    if isinstance(parsed, exp.Insert):

        # 如果系统配置不允许管理类写操作，直接拒绝
        if not ENABLE_ADMIN_OPS:
            raise ValueError("Admin operations (INSERT) are currently disabled.")

        # 允许时，直接返回标准化后的 SQL
        return parsed.sql(dialect="mysql")

    # 如果是 UPDATE 或 DELETE
    if isinstance(parsed, (exp.Update, exp.Delete)):

        # 同样先检查是否允许管理类操作
        if not ENABLE_ADMIN_OPS:
            raise ValueError(f"Admin operations ({parsed.key.upper()}) are currently disabled.")

        # 关键保护：
        # UPDATE / DELETE 必须带 WHERE
        # 否则很容易误伤整张表
        if parsed.args.get("where") is None:
            raise ValueError(f"{parsed.key.upper()} without WHERE is not allowed.")

        # 校验通过后，返回标准化 SQL
        return parsed.sql(dialect="mysql")

    # 其他任何类型的 SQL（比如 DDL：DROP / ALTER / CREATE 等）一律拒绝
    raise ValueError(f"Unsupported or highly dangerous SQL operation: {parsed.key.upper()}")


# 这个方法的作用：
# 一个内部辅助函数。
# 专门负责把 SQL 字符串解析成 AST，并强制要求“只能有一条 SQL 语句”。
# 这样可以避免分号拼接多条语句的风险。
def _parse_single_statement(sql: str) -> exp.Expression:
    """
    解析 SQL，并确保里面只有一条 statement。
    """

    # 按 MySQL 方言解析 SQL
    statements = sqlglot.parse(sql, read="mysql")

    # 如果解析出来不是恰好一条语句，就报错
    # 例如：
    # SELECT * FROM a; DELETE FROM b;
    # 这种会被拦住
    if len(statements) != 1:
        raise ValueError("Only a single SQL statement is allowed.")

    # 返回唯一那条 AST 表达式
    return statements[0]


# 这个方法的作用：
# 把传进来的 schema_context 文本，解析成结构化的 schema_map。
#
# 输入通常类似：
# Table: users
# Columns: id int, name varchar, email varchar
#
# Table: orders
# Columns: id int, user_id int, total decimal
#
# 输出会变成：
# {
#   "users": {"id", "name", "email"},
#   "orders": {"id", "user_id", "total"}
# }
def _extract_schema_map(schema_context: str) -> dict[str, set[str]]:
    """
    把纯文本格式的 schema 描述，解析成：
    {表名: {列名1, 列名2, ...}}
    """

    # 初始化最终结果字典
    schema_map: dict[str, set[str]] = {}

    # current_table 表示“当前正在解析的是哪张表”
    # 初始时还没遇到 Table: 行，所以为 None
    current_table: str | None = None

    # 按行遍历 schema 文本
    for raw_line in schema_context.splitlines():

        # 去掉每行首尾空白字符
        line = raw_line.strip()

        # 空行直接跳过
        if not line:
            continue

        # 如果当前行以 "Table:" 开头，说明定义了一张新表
        if line.lower().startswith("table:"):

            # 冒号后面的内容就是表名
            # 取出后去空格并统一转小写
            current_table = line.split(":", 1)[1].strip().lower()

            # 如果表名不为空，就在 schema_map 里先为它建一个空 set
            if current_table:
                schema_map[current_table] = set()

            # 当前行处理完，继续下一行
            continue

        # 如果当前行以 "Columns:" 开头，并且已经有 current_table
        # 说明这是在给当前表补列信息
        if line.lower().startswith("columns:") and current_table:

            # 取出 "Columns:" 后面的内容
            cols_part = line.split(":", 1)[1].strip()

            # 按逗号拆成多个字段项
            for item in cols_part.split(","):

                # 每个 item 可能长这样：
                # "id int"
                # "name varchar"
                # 这里只取第一个空格前面的列名
                col_name = item.strip().split(" ", 1)[0].strip().lower()

                # 如果列名非空，就加入当前表对应的列集合
                if col_name:
                    schema_map[current_table].add(col_name)

    # 返回解析后的 schema 映射
    return schema_map


# 这个方法的作用：
# 做第二层“语义守卫”。
#
# 它不是单纯看 SQL 语法，而是检查：
# 1. SQL 里引用的表，是否在 schema_context 中存在
# 2. SQL 里引用的列，是否在对应表中存在
# 3. 表别名是否能正确映射到真实表
#
# 主要是防止 LLM 生成“看起来像 SQL，但其实引用了不存在字段/表”的幻觉查询。
def semantic_guard(question: str, sql: str, schema_context: str) -> tuple[bool, str | None]:
    """
    检查生成的 SQL 是否与提供的 schema 上下文语义一致。

    参数：
        question:
            原始自然语言问题
            当前版本里没有实际使用，先保留为后续扩展
        sql:
            待校验的 SQL
        schema_context:
            提供给模型的 schema 文本上下文

    返回：
        (是否通过, 失败原因)
        例如：
            (True, None)
            (False, "Schema context is empty.")
    """

    # 当前版本没有真正使用 question
    # 这里显式删除它，避免“未使用变量”的提示，也表明这是有意保留的参数
    del question

    # 如果 schema_context 为空，直接失败
    if not schema_context or not schema_context.strip():
        return False, "Schema context is empty."

    # 先把 schema 文本解析成结构化映射
    schema_map = _extract_schema_map(schema_context)

    # 如果解析不出任何表，也视为失败
    if not schema_map:
        return False, "Schema context could not be parsed."

    # 先做一轮基于 sqlglot qualify 的强校验
    try:
        # 把 SQL 解析成 AST，并确保只有一条语句
        parsed = _parse_single_statement(sql)

        # 用 qualify 做“基于 schema 的 SQL 解析/限定”
        # 这里传入的 schema 格式是：
        # {
        #   "table_name": {
        #       "col1": "UNKNOWN",
        #       "col2": "UNKNOWN"
        #   }
        # }
        #
        # 类型值这里写 UNKNOWN 就够了，因为这里只关心“表/列能不能被识别”
        qualify(
            parsed.copy(),
            dialect="mysql",
            schema={t: {c: "UNKNOWN" for c in cols} for t, cols in schema_map.items()},
        )

    # qualify 或解析过程中一旦报错，就说明 SQL 和 schema 对不上
    except Exception as e:
        return False, f"SQL validation/qualification failed: {e}"

    # alias_to_table:
    # 记录“表别名 -> 真实表名”的映射
    # 例如：
    # SELECT u.name FROM users u
    # 则 alias_to_table["u"] = "users"
    alias_to_table: dict[str, str] = {}

    # referenced_tables:
    # 记录 SQL 实际引用了哪些表
    referenced_tables: set[str] = set()

    # 遍历 SQL 中所有 Table 节点，收集表和别名信息
    for table in parsed.find_all(exp.Table):

        # 没有表名的节点跳过
        if not table.name:
            continue

        # 真实表名统一转小写
        real_name = table.name.lower()

        # 加入已引用表集合
        referenced_tables.add(real_name)

        # 如果有别名就取别名，否则直接用表名本身
        alias_name = table.alias_or_name.lower() if table.alias_or_name else real_name

        # 建立别名到真实表名的映射
        alias_to_table[alias_name] = real_name

    # 检查 SQL 中引用的表，有没有不在 schema_context 里的
    unknown_tables = referenced_tables - set(schema_map.keys())

    # 只要存在未知表，就返回失败
    if unknown_tables:
        return False, f"SQL references tables not present in schema context: {sorted(unknown_tables)}"

    # 再逐个检查 SQL 中引用到的列是否合法
    for column in parsed.find_all(exp.Column):

        # 取列名并统一转小写
        column_name = column.name.lower() if column.name else None

        # 如果列名不存在，或者是 *，就跳过
        # 因为 * 不适合用普通列名规则校验
        if not column_name or column_name == "*":
            continue

        # qualifier 表示列前面的表名/别名
        # 例如 u.name 里的 u
        qualifier = column.table.lower() if column.table else None

        # 情况 1：这是“带表前缀”的列，比如 u.name
        if qualifier:

            # 先把别名解析成真实表名
            # 如果 alias_to_table 里没有，就退回直接把 qualifier 当真实表名
            real_table = alias_to_table.get(qualifier, qualifier)

            # 取出该表允许的列集合
            allowed_cols = schema_map.get(real_table)

            # 如果连这张表都不认识，说明 qualifier 指向有问题
            if allowed_cols is None:
                return False, f"Column '{column.sql()}' references unknown table/alias '{qualifier}'."

            # 如果列不在该表的字段集合中，返回失败
            if column_name not in allowed_cols:
                return False, f"Column '{column.sql()}' is not present in table '{real_table}'."

        # 情况 2：这是“不带表前缀”的列，比如 name
        else:
            # 如果 SQL 已经引用了具体表，就只在这些表里找
            # 否则退化为在整个 schema_map 里找
            search_tables = referenced_tables or set(schema_map.keys())

            # 只要这些表里没有任何一个包含该列，就失败
            if not any(column_name in schema_map.get(t, set()) for t in search_tables):
                return False, f"Unqualified column '{column_name}' was not found in referenced tables."

    # 所有检查都通过，返回成功
    return True, None







# # -*- coding: utf-8 -*-

# """
# app/services/guard_service.py

# This module implements the safety and validation layer for the SQL generation process.

# It provides two main functions:
# 1. `validate_sql`: Performs syntactic and security checks on a generated SQL query.
#    This includes preventing destructive operations (unless explicitly enabled),
#    enforcing limits, and ensuring only allowed tables are accessed.
# 2. `semantic_guard`: Verifies that the generated SQL is semantically consistent
#    with the database schema context provided to the LLM. It ensures the SQL only
#    references tables and columns that were part of its known context, preventing
#    hallucinations or queries based on incorrect assumptions.
# """

# import sqlglot
# from sqlglot import exp
# from sqlglot.optimizer.qualify import qualify
# from app.config import ENABLE_ADMIN_OPS

# from collections.abc import Collection


# def validate_sql(sql: str, allowed_tables: Collection[str] | None = None) -> str:
#     """
#     Parses, validates, and standardizes a single SQL statement.

#     This function acts as a critical security checkpoint:
#     - Ensures the input is a single, valid SQL statement.
#     - Checks that it only accesses tables from an approved list (if provided).
#     - Prevents destructive operations (UPDATE, DELETE, INSERT) unless globally enabled.
#     - Rejects UPDATE/DELETE statements that lack a WHERE clause.
#     - Automatically adds a `LIMIT 100` clause to SELECT queries to prevent excessive data retrieval.

#     Args:
#         sql (str): The raw SQL string to validate.
#         allowed_tables (Collection[str], optional): A set of table names that are permissible to query.

#     Raises:
#         ValueError: If the SQL is invalid, uses forbidden tables, or performs a disallowed operation.

#     Returns:
#         str: The validated, standardized, and potentially modified SQL string.
#     """
#     try:
#         parsed = _parse_single_statement(sql)
#     except Exception as e:
#         raise ValueError(f"Failed to parse SQL: {e}")

#     tables = {t.name.lower() for t in parsed.find_all(exp.Table) if t.name}

#     if not tables:
#         raise ValueError("SQL must reference at least one table.")

#     # Check if the query accesses tables outside the allowed set.
#     if allowed_tables is not None:
#         normalized_allowed_tables = {t.lower() for t in allowed_tables}
#         if not tables.issubset(normalized_allowed_tables):
#             raise ValueError(f"Forbidden tables detected: {tables - normalized_allowed_tables}")

#     # Handle SELECT queries
#     if isinstance(parsed, exp.Query):
#         if parsed.args.get("limit") is None:
#             # Add a default limit to prevent accidental large queries.
#             parsed = parsed.limit(100)
#         return parsed.sql(dialect="mysql")

#     # Handle INSERT operations
#     if isinstance(parsed, exp.Insert):
#         if not ENABLE_ADMIN_OPS:
#             raise ValueError("Admin operations (INSERT) are currently disabled.")
#         return parsed.sql(dialect="mysql")

#     # Handle UPDATE and DELETE operations
#     if isinstance(parsed, (exp.Update, exp.Delete)):
#         if not ENABLE_ADMIN_OPS:
#             raise ValueError(f"Admin operations ({parsed.key.upper()}) are currently disabled.")
#         # Crucial safety check: require a WHERE clause for modifications.
#         if parsed.args.get("where") is None:
#             raise ValueError(f"{parsed.key.upper()} without WHERE is not allowed.")
#         return parsed.sql(dialect="mysql")

#     # Reject any other type of statement (e.g., DDL).
#     raise ValueError(f"Unsupported or highly dangerous SQL operation: {parsed.key.upper()}")


# def _parse_single_statement(sql: str) -> exp.Expression:
#     """
#     A helper function to parse a SQL string and ensure it contains exactly one statement.
#     """
#     statements = sqlglot.parse(sql, read="mysql")
#     if len(statements) != 1:
#         raise ValueError("Only a single SQL statement is allowed.")
#     return statements[0]


# def _extract_schema_map(schema_context: str) -> dict[str, set[str]]:
#     """
#     Parses the plain-text schema context string into a structured dictionary.

#     The schema context is expected in a specific format (e.g., from `schema_service`).
#     This function converts it into a map of `{'table_name': {'col1', 'col2', ...}}`.

#     Args:
#         schema_context (str): The string representation of the schema.

#     Returns:
#         dict: A map of table names to a set of their column names.
#     """
#     schema_map: dict[str, set[str]] = {}
#     current_table: str | None = None

#     for raw_line in schema_context.splitlines():
#         line = raw_line.strip()
#         if not line:
#             continue

#         if line.lower().startswith("table:"):
#             current_table = line.split(":", 1)[1].strip().lower()
#             if current_table:
#                 schema_map[current_table] = set()
#             continue

#         if line.lower().startswith("columns:") and current_table:
#             cols_part = line.split(":", 1)[1].strip()
#             for item in cols_part.split(","):
#                 col_name = item.strip().split(" ", 1)[0].strip().lower()
#                 if col_name:
#                     schema_map[current_table].add(col_name)

#     return schema_map


# def semantic_guard(question: str, sql: str, schema_context: str) -> tuple[bool, str | None]:
#     """
#     Checks if the generated SQL is semantically valid against the provided schema context.

#     This guard ensures that the LLM-generated query only uses tables and columns
#     that it was "shown" in the prompt. It helps catch LLM "hallucinations" where
#     the model might invent tables or columns that do not exist in the context.

#     Args:
#         question (str): The original natural language question (currently unused, for future use).
#         sql (str): The generated SQL query.
#         schema_context (str): The schema information that was provided to the LLM.

#     Returns:
#         tuple[bool, str | None]: A tuple containing a boolean indicating if the guard passed,
#                                  and a reason string if it failed.
#     """
#     # The 'question' argument is kept for potential future use, e.g., for more advanced semantic checks.
#     del question

#     if not schema_context or not schema_context.strip():
#         return False, "Schema context is empty."

#     schema_map = _extract_schema_map(schema_context)
#     if not schema_map:
#         return False, "Schema context could not be parsed."

#     try:
#         parsed = _parse_single_statement(sql)

#         # Use sqlglot's `qualify` function as a powerful tool to validate the query
#         # against the known schema. This will raise an error if tables or columns
#         # cannot be resolved.
#         qualify(
#             parsed.copy(),
#             dialect="mysql",
#             schema={t: {c: "UNKNOWN" for c in cols} for t, cols in schema_map.items()},
#         )
#     except Exception as e:
#         return False, f"SQL validation/qualification failed: {e}"

#     # The following manual checks provide more specific error messages,
#     # although `qualify` handles the core validation.

#     alias_to_table: dict[str, str] = {}
#     referenced_tables: set[str] = set()

#     # Find all referenced tables and their aliases.
#     for table in parsed.find_all(exp.Table):
#         if not table.name:
#             continue
#         real_name = table.name.lower()
#         referenced_tables.add(real_name)

#         alias_name = table.alias_or_name.lower() if table.alias_or_name else real_name
#         alias_to_table[alias_name] = real_name

#     # Check for tables not in the context.
#     unknown_tables = referenced_tables - set(schema_map.keys())
#     if unknown_tables:
#         return False, f"SQL references tables not present in schema context: {sorted(unknown_tables)}"

#     # Check each column to ensure it exists in its referenced table.
#     for column in parsed.find_all(exp.Column):
#         column_name = column.name.lower() if column.name else None
#         if not column_name or column_name == "*":
#             continue

#         qualifier = column.table.lower() if column.table else None
#         if qualifier:
#             # Column is qualified (e.g., `t.col`).
#             real_table = alias_to_table.get(qualifier, qualifier)
#             allowed_cols = schema_map.get(real_table)
#             if allowed_cols is None:
#                 return False, f"Column '{column.sql()}' references unknown table/alias '{qualifier}'."
#             if column_name not in allowed_cols:
#                 return False, f"Column '{column.sql()}' is not present in table '{real_table}'."
#         else:
#             # Column is unqualified. Check if it exists in any of the referenced tables.
#             search_tables = referenced_tables or set(schema_map.keys())
#             if not any(column_name in schema_map.get(t, set()) for t in search_tables):
#                 return False, f"Unqualified column '{column_name}' was not found in referenced tables."

#     return True, None
