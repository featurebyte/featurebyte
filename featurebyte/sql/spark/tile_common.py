"""
Base Class for Tile Schedule Instance
"""
from typing import Any

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel
from pyspark.sql import SparkSession


class TileCommon(BaseModel):

    featurebyte_database: str
    tile_id: str
    tile_modulo_frequency_second: int
    blind_spot_second: int
    frequency_minute: int

    sql: str
    entity_column_names: str
    value_column_names: str
    value_column_types: str

    _spark: SparkSession = PrivateAttr()

    def __init__(self, spark_session: SparkSession, **kwargs: Any):
        """
        Initialize Tile Schedule Instance

        Parameters
        ----------
        spark_session: SparkSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(**kwargs)
        self._spark = spark_session
        self._spark.sql(f"USE DATABASE {self.featurebyte_database}")
