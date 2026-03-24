# -*- coding: utf-8 -*-

"""
app/models/schemas.py

This module defines the Pydantic models used for data validation and
serialization throughout the application.

Pydantic models ensure that incoming request data conforms to the expected
structure and data types, providing a clear and reliable data contract
for API endpoints.
"""

from pydantic import BaseModel


class QueryRequest(BaseModel):
    """
    Represents the request body for a text-to-SQL query.

    Attributes:
        question (str): The natural language question to be converted into a SQL query.
    """
    question: str
