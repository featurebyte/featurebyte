"""
Tile Generate online store Job Script for SP_TILE_SCHEDULE_ONLINE_STORE
"""
from typing import Any

from pydantic.fields import PrivateAttr
from pydantic.main import BaseModel

from featurebyte.logger import logger
from featurebyte.session.base import BaseSession


class TileScheduleOnlineStore(BaseModel):
    """
    Tile Schedule Online Store script corresponding to SP_TILE_SCHEDULE_ONLINE_STORE stored procedure
    """

    agg_id: str
    job_schedule_ts_str: str

    _spark: BaseSession = PrivateAttr()

    def __init__(self, spark_session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Schedule Online Store Instance

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
        Execute tile schedule online store operation
        """

        select_sql = f"""
            SELECT
              RESULT_ID,
              SQL_QUERY,
              ONLINE_STORE_TABLE_NAME,
              ENTITY_COLUMN_NAMES,
              RESULT_TYPE
            FROM ONLINE_STORE_MAPPING
            WHERE
              AGGREGATION_ID ILIKE '{self.agg_id}' AND IS_DELETED = FALSE
        """

        online_store_df = await self._spark.execute_query(select_sql)
        if online_store_df is None or len(online_store_df) == 0:
            return

        for _, row in online_store_df.iterrows():
            f_name = row["RESULT_ID"]
            f_sql = row["SQL_QUERY"]
            fs_table = row["ONLINE_STORE_TABLE_NAME"]
            f_entity_columns = row["ENTITY_COLUMN_NAMES"]
            f_value_type = row["RESULT_TYPE"]
            f_sql = f_sql.replace(
                "__FB_POINT_IN_TIME_SQL_PLACEHOLDER", "'" + self.job_schedule_ts_str + "'"
            )

            logger.debug(f"{f_name}, {fs_table}, {f_entity_columns}, {f_value_type}")

            fs_table_exist_flag = True
            try:
                await self._spark.execute_query(f"select * from {fs_table} limit 1")
            except Exception:  # pylint: disable=broad-except
                fs_table_exist_flag = False
            logger.debug(f"fs_table_exist_flag: {fs_table_exist_flag}")

            if not fs_table_exist_flag:
                # feature store table does not exist, create table with the input feature sql
                columns = f_entity_columns.split(",")
                columns.append(f_name)
                columns_str = ", ".join(columns)
                create_sql = (
                    f"create table {fs_table} using delta as (select {columns_str} from ({f_sql}))"
                )
                await self._spark.execute_query(create_sql)

            else:
                # feature store table already exists, insert records with the input feature sql
                entity_insert_cols = []
                entity_filter_cols = []
                for element in f_entity_columns.split(","):
                    entity_insert_cols.append("b." + element)
                    entity_filter_cols.append("a." + element + " = b." + element)

                entity_insert_cols_str = ",".join(entity_insert_cols)
                entity_filter_cols_str = " AND ".join(entity_filter_cols)

                # check whether feature value column exists, if not add the new column
                try:
                    await self._spark.execute_query(f"SELECT {f_name} FROM {fs_table} LIMIT 1")
                except Exception:  # pylint: disable=broad-except
                    await self._spark.execute_query(
                        f"ALTER TABLE {fs_table} ADD COLUMN {f_name} {f_value_type}"
                    )

                # TODO: local spark does not support update subquery
                # remove feature values for entities that are not in entity universe
                # remove_values_sql = f"""
                #     update {fs_table} a set a.{f_name} = NULL
                #         WHERE NOT EXISTS
                #         (select * from ({f_sql}) b WHERE {entity_filter_cols_str})
                # """

                # update or insert feature values for entities that are in entity universe
                merge_sql = f"""
                    merge into {fs_table} a using ({f_sql}) b
                        on {entity_filter_cols_str}
                        when matched then
                            update set a.{f_name} = b.{f_name}
                        when not matched then
                            insert ({f_entity_columns}, {f_name})
                                values ({entity_insert_cols_str}, b.{f_name})
                """
                await self._spark.execute_query(merge_sql)
