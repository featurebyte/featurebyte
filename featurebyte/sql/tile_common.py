"""
Base Class for Tile Schedule Instance
"""
from typing import Any, List

from abc import ABC, abstractmethod

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel

from featurebyte.session.base import BaseSession
from featurebyte.session.spark import SparkSession


class TileCommon(BaseModel, ABC):
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

    _spark: BaseSession = PrivateAttr()

    def __init__(self, spark_session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Operation Instance

        Parameters
        ----------
        spark_session: BaseSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(**kwargs)
        self._spark = spark_session

    @property
    def entity_column_names_str(self) -> str:
        """
        Format entity_column_names into comma-separated string

        Returns
        -------
            string representation of entity_column_names
        """

        if isinstance(self._spark, SparkSession):
            return ",".join([f"`{col}`" for col in self.entity_column_names])

        return ",".join([f'"{col}"' for col in self.entity_column_names])

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
