"""
Base Class for Tile Schedule Instance
"""
from typing import Any, List

from abc import ABC, abstractmethod

from featurebyte.session.base import BaseSession
from featurebyte.sql.base import BaselSqlModel


class TileCommon(BaselSqlModel, ABC):
    """
    Base class for Tile Operation Classes
    """

    tile_id: str
    aggregation_id: str
    tile_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int

    sql: str
    entity_column_names: List[str]
    value_column_names: List[str]
    value_column_types: List[str]

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
        super().__init__(session=session, **kwargs)

    @property
    def entity_column_names_str(self) -> str:
        """
        Format entity_column_names into comma-separated string

        Returns
        -------
            string representation of entity_column_names
        """
        return ",".join([self.quote_column(col) for col in self.entity_column_names])

    @property
    def value_column_names_str(self) -> str:
        """
        Format value_column_names into comma-separated string

        Returns
        -------
            string representation of value_column_names
        """

        return ",".join(self.value_column_names)

    @property
    def value_column_types_str(self) -> str:
        """
        Format value_column_types into comma-separated string

        Returns
        -------
            string representation of value_column_types
        """

        return ",".join(self.value_column_types)

    @abstractmethod
    async def execute(self) -> None:
        """
        Base abstract method for tile related subclass
        """
