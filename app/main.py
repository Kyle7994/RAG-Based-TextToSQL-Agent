# -*- coding: utf-8 -*-

"""
app/main.py

This module serves as the entry point for the FastAPI application.

It initializes the FastAPI app, sets a descriptive title, and includes the
main API router from `app.api.routes`. This setup creates the foundation
for the entire web service, making it ready to receive and handle HTTP requests.
"""

from fastapi import FastAPI
from app.api.routes import router

# Initialize the FastAPI application instance.
# The title will be displayed in the API documentation (e.g., Swagger UI).
app = FastAPI(title="RAG-Based Text-to-SQL Agent")

# Include the API router.
# This attaches all the endpoints defined in `app.api.routes.router` to the main application,
# making them accessible to clients.
app.include_router(router)
