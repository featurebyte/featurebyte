"""
Common utilities for Spark SQL
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.session.base import BaseSession

logger = get_logger(__name__)


@asynccontextmanager
async def register_temporary_physical_table(session: BaseSession, query: str) -> AsyncIterator[str]:
    """
    Register a temporary physical table using a Select query

    Parameters
    ----------
    session : BaseSession
        Session objectd
    query : str
        SQL query for the table

    Yields
    ------
    str
        The name of the temporary table
    """
    table_name = f"__SESSION_TEMP_TABLE_{ObjectId()}".upper()
    await session.create_table_as(
        table_details=table_name,
        select_expr=query,
    )
    try:
        yield table_name
    finally:
        await session.drop_table(
            table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
            if_exists=True,
        )
