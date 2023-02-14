"""
Tile Generate entity tracking Job script for SP_TILE_GENERATE_ENTITY_TRACKING
"""
from typing import Any

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel
from pyspark.sql import SparkSession

from featurebyte.logger import logger


class TileGenerateEntityTracking(BaseModel):
    featurebyte_database: str
    tile_last_start_date_column: str
    entity_column_names: str
    tile_id: str
    entity_table: str

    _spark: SparkSession = PrivateAttr()

    def __init__(self, spark_session: SparkSession, **kwargs: Any):
        """
        Initialize Tile Generate Entity Tracking

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

    def execute(self) -> None:

        tracking_table_name = self.tile_id + "_ENTITY_TRACKER"
        tracking_table_exist = self._spark.catalog.tableExists(tracking_table_name)

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names.split(","):
            element = element.strip()
            entity_insert_cols.append("b." + element)
            entity_filter_cols.append("a." + element + " = b." + element)

        entity_insert_cols_str = ",".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)
        logger.debug(f"entity_insert_cols_str: {entity_insert_cols_str}")
        logger.debug(f"entity_filter_cols_str: {entity_filter_cols_str}")

        # create table or insert new records or update existing records
        if not tracking_table_exist:
            create_sql = f"create table {tracking_table_name} as SELECT * FROM {self.entity_table}"
            self._spark.sql(create_sql)
        else:
            merge_sql = f"""
                merge into {tracking_table_name} a using {self.entity_table} b
                    on {entity_filter_cols_str}
                    when matched then
                        update set a.{self.tile_last_start_date_column} = b.{self.tile_last_start_date_column}
                    when not matched then
                        insert ({self.entity_column_names}, {self.tile_last_start_date_column})
                            values ({entity_insert_cols_str}, b.{self.tile_last_start_date_column})
            """
            self._spark.sql(merge_sql)
