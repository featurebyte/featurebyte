"""
Base Class for SQL related operations
"""

from typing import Any

from pydantic import BaseModel, PrivateAttr

from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


class BaseSqlModel(BaseModel):
    """
    Base class for Tile Operation Classes
    """

    _session: BaseSession = PrivateAttr()

    def __init__(self, session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Operation Instance

        Parameters
        ----------
        session: BaseSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(**kwargs)
        self._session = session

    @property
    def adapter(self) -> BaseAdapter:
        """
        Get SQL adapter based on session type

        Returns
        -------
        BaseAdapter
        """
        return get_sql_adapter(self._session.source_type)

    def quote_column(self, col_val: str) -> str:
        """
        Quote column name based on session type

        Parameters
        ----------
        col_val: str
            input column name

        Returns
        -------
            quoted column name
        """
        return sql_to_string(quoted_identifier(col_val), self._session.source_type)

    def quote_column_null_aware_equal(self, left_expr: str, right_expr: str) -> str:
        """
        Compares whether two expressions are null-safe equal

        Parameters
        ----------
        left_expr: str
            left expression
        right_expr: str
            right expression

        Returns
        -------
            null aware equal expression
        """
        if isinstance(self._session, SnowflakeSession):
            return f"EQUAL_NULL({left_expr}, {right_expr})"

        return f"{left_expr} <=> {right_expr}"

    async def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists

        Parameters
        ----------
        table_name: str
            input table name

        Returns
        -------
            True if table exists, False otherwise
        """
        return await self._session.table_exists(table_name)
