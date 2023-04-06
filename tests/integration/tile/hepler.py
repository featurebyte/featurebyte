"""
This module contains utility functions used in tests
"""
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


def format_timestamp_expr(session: BaseSession, col_name: str) -> str:
    """
    Format timestamp expression based on session type

    Parameters
    ----------
    session: BaseSession
        input session
    col_name: str
        input timestamp column name

    Returns
    -------
        formatted timestamp expression
    """
    if isinstance(session, SnowflakeSession):
        return f"to_varchar({col_name}, 'YYYY-MM-DD HH24:MI:SS')"

    return f"date_format({col_name}, 'yyyy-MM-dd HH:mm:ss')"
