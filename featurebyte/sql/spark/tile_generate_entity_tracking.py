"""
Tile Generate entity tracking Job script for SP_TILE_GENERATE_ENTITY_TRACKING
"""
from typing import Any, List

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel

from featurebyte.logger import logger
from featurebyte.session.base import BaseSession


class TileGenerateEntityTracking(BaseModel):
    """
    Tile Generate entity tracking script corresponding to SP_TILE_GENERATE_ENTITY_TRACKING stored procedure
    """

    tile_last_start_date_column: str
    entity_column_names: List[str]
    tile_id: str
    entity_table: str

    _spark: BaseSession = PrivateAttr()

    def __init__(self, spark_session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Generate Entity Tracking

        Parameters
        ----------
        spark_session: BaseSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(**kwargs)
        self._spark = spark_session

    async def execute(self) -> None:
        """
        Execute tile generate entity tracking operation
        """

        tracking_table_name = self.tile_id + "_ENTITY_TRACKER"
        tracking_table_exist_flag = True
        try:
            await self._spark.execute_query(f"select * from {tracking_table_name} limit 1")
        except Exception:  # pylint: disable=broad-except
            tracking_table_exist_flag = False

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names:
            element = element.strip()
            entity_insert_cols.append("b." + element)
            entity_filter_cols.append("a." + element + " = b." + element)

        entity_insert_cols_str = ",".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)
        logger.debug(f"entity_insert_cols_str: {entity_insert_cols_str}")
        logger.debug(f"entity_filter_cols_str: {entity_filter_cols_str}")

        # create table or insert new records or update existing records
        if not tracking_table_exist_flag:
            create_sql = f"create table {tracking_table_name} using delta as select * from {self.entity_table}"
            await self._spark.execute_query(create_sql)
        else:
            entity_column_names_str = ",".join(self.entity_column_names)
            merge_sql = f"""
                merge into {tracking_table_name} a using {self.entity_table} b
                    on {entity_filter_cols_str}
                    when matched then
                        update set a.{self.tile_last_start_date_column} = b.{self.tile_last_start_date_column}
                    when not matched then
                        insert ({entity_column_names_str}, {self.tile_last_start_date_column})
                            values ({entity_insert_cols_str}, b.{self.tile_last_start_date_column})
            """
            await self._spark.execute_query(merge_sql)
