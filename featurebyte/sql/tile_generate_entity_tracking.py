"""
Tile Generate entity tracking Job script for SP_TILE_GENERATE_ENTITY_TRACKING
"""
from typing import Any, List

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel

from featurebyte.logger import logger
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.sql.common import construct_create_delta_table_query, retry_sql_with_cache


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
        escaped_entity_column_names = []
        for element in self.entity_column_names:
            element = element.strip()
            if isinstance(self._spark, SnowflakeSession):
                entity_insert_cols.append(f'b."{element}"')
                entity_filter_cols.append(f'EQUAL_NULL(a."{element}", b."{element}")')
                escaped_entity_column_names.append(f'"{element}"')
            else:
                entity_insert_cols.append(f"b.`{element}`")
                entity_filter_cols.append(f"a.`{element}` <=> b.`{element}`")
                escaped_entity_column_names.append(f"`{element}`")

        escaped_entity_column_names_str = ",".join(escaped_entity_column_names)

        entity_insert_cols_str = ", ".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)
        logger.debug(f"entity_insert_cols_str: {entity_insert_cols_str}")
        logger.debug(f"entity_filter_cols_str: {entity_filter_cols_str}")

        # create table or insert new records or update existing records
        if not tracking_table_exist_flag:
            create_sql = construct_create_delta_table_query(
                tracking_table_name, self.entity_table, session=self._spark
            )
            logger.debug(f"create_sql: {create_sql}")
            await self._spark.execute_query(create_sql)
        else:
            merge_sql = f"""
                merge into {tracking_table_name} a using ({self.entity_table}) b
                    on {entity_filter_cols_str}
                    when matched then
                        update set a.{self.tile_last_start_date_column} = b.{self.tile_last_start_date_column}
                    when not matched then
                        insert ({escaped_entity_column_names_str}, {self.tile_last_start_date_column})
                            values ({entity_insert_cols_str}, b.{self.tile_last_start_date_column})
            """
            await retry_sql_with_cache(
                session=self._spark, sql=merge_sql, cached_select_sql=self.entity_table
            )
