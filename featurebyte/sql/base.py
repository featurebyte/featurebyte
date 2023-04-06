"""
Base Class for SQL related operations
"""
from typing import Any, List

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel

from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession


class BaselSqlModel(BaseModel):
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
        if isinstance(self._session, SnowflakeSession):
            return f'"{col_val}"'

        return f"`{col_val}`"

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

    @property
    def schema_column_name(self) -> str:
        """
        Column name for schema based on session type

        Returns
        -------
            column name for schema
        """
        if isinstance(self._session, SnowflakeSession):
            return "column_name"

        return "col_name"

    async def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names for a table. The column names are returned in upper case.

        Parameters
        ----------
        table_name: str
            input table name

        Returns
        -------
            list of column names in upper case
        """
        cols_df = await self._session.execute_query(f"SHOW COLUMNS IN {table_name}")
        cols = []
        if cols_df is not None:
            for _, row in cols_df.iterrows():
                cols.append(row[self.schema_column_name].upper())
        return cols

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
        table_exist_flag = True
        try:
            await self._session.execute_query(f"select * from {table_name} limit 1")
        except self._session._no_schema_error:  # pylint: disable=protected-access
            table_exist_flag = False

        return table_exist_flag
