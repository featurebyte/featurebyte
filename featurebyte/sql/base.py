"""
Base Class for SQL related operations
"""
from typing import Any

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
