# -*- coding: utf-8 -*-

"""
app/services/mysql_service.py

This module provides an interface for interacting with the MySQL database.

It is responsible for:
- Establishing resilient database connections with automatic retry logic.
- Executing SQL queries against the database.
- Normalizing data types from query results into a consistent,
  JSON-serializable format.
"""

import pymysql
from pymysql.err import InterfaceError, OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from app.config import MYSQL_DB, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT, MYSQL_USER
from datetime import date, datetime
from decimal import Decimal


@retry(
    stop=stop_after_attempt(3),  # Stop after 3 attempts.
    wait=wait_exponential_jitter(initial=1, max=5),  # Exponential backoff with jitter.
    retry=retry_if_exception_type((OperationalError, InterfaceError)),  # Retry on specific transient DB errors.
    reraise=True,  # Re-raise the last exception if all retries fail.
)
def get_conn():
    """
    Establishes and returns a connection to the MySQL database.

    This function is decorated with a retry mechanism to handle common transient
    database connection errors, making the service more resilient.

    Returns:
        pymysql.Connection: A database connection object.
    """
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.Cursor,
        autocommit=True,
        charset="utf8mb4",
        connect_timeout=5,
        read_timeout=30,
        write_timeout=30,
    )


def _normalize_query_value(value):
    """
    A helper function to normalize database values for JSON serialization.

    It converts data types like `Decimal` and `datetime` into string
    representations, which are safe for JSON encoding.

    Args:
        value: The value from a database row.

    Returns:
        The normalized value (e.g., a string or the original value).
    """
    if isinstance(value, Decimal):
        # Convert Decimal to string to preserve precision.
        return str(value)
    if isinstance(value, (datetime, date)):
        # Convert date/datetime objects to ISO 8601 format string.
        return value.isoformat()
    return value


def run_query(sql: str):
    """
    Executes a given SQL query and returns the results.

    It acquires a database connection, executes the query, and fetches all
    results. It also extracts column headers and normalizes the row data.

    Args:
        sql (str): The SQL query to execute.

    Returns:
        A tuple containing:
        - list[str]: A list of column names.
        - list[list]: A list of rows, where each row is a list of normalized values.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            raw_rows = cur.fetchall()
            
            # Extract column names from the cursor description.
            columns = [desc[0] for desc in cur.description] if cur.description else []

            # Normalize each cell in each row for consistent output.
            rows = [
                [_normalize_query_value(cell) for cell in row]
                for row in raw_rows
            ]

            return columns, rows
    finally:
        # Ensure the connection is always closed.
        conn.close()
