#!/usr/bin/env python3
"""Main entry point for Heritage Assets API"""

import uvicorn

from app.api import app
from app.database import engine
from app.models import create_tables
from config import settings


def main():
    """Run the API server"""
    # Ensure tables exist
    create_tables(engine)

    # Run uvicorn
    uvicorn.run(
        "app.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
