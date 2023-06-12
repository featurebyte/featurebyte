"""
Tile Generate online store Job Script
"""
from typing import Any, Union

from datetime import datetime

import pandas as pd
from pydantic import Field

from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.session.base import BaseSession
from featurebyte.sql.base import BaselSqlModel
from featurebyte.sql.common import construct_create_table_query, retry_sql

logger = get_logger(__name__)


class TileScheduleOnlineStore(BaselSqlModel):
    """
    Tile Schedule Online Store script
    """

    aggregation_id: str
    job_schedule_ts_str: str
    retry_num: int = Field(default=10)

    async def retry_sql(self, sql: str) -> Union[pd.DataFrame, None]:
        """
        Execute sql query with retry

        Parameters
        ----------
        sql: str
            SQL query

        Returns
        -------
        pd.DataFrame | None
        """
        return await retry_sql(self._session, sql, retry_num=self.retry_num)

    def __init__(self, session: BaseSession, **kwargs: Any):
        """
        Initialize Tile Schedule Online Store Instance

        Parameters
        ----------
        session: BaseSession
            input SparkSession
        kwargs: Any
            constructor arguments
        """
        super().__init__(session=session, **kwargs)

    async def execute(self) -> None:
        """
        Execute tile schedule online store operation
        """
        # pylint: disable=too-many-locals,too-many-statements
        select_sql = f"""
            SELECT
              RESULT_ID,
              SQL_QUERY,
              ONLINE_STORE_TABLE_NAME,
              ENTITY_COLUMN_NAMES,
              RESULT_TYPE
            FROM ONLINE_STORE_MAPPING
            WHERE
              AGGREGATION_ID ILIKE '{self.aggregation_id}' AND IS_DELETED = FALSE
        """
        online_store_df = await self.retry_sql(select_sql)
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

            # check if feature store table exists
            fs_table_exist_flag = await self.table_exists(fs_table)
            logger.debug(f"fs_table_exist_flag: {fs_table_exist_flag}")

            quoted_result_name_column = self.quote_column(
                InternalName.ONLINE_STORE_RESULT_NAME_COLUMN
            )
            quoted_value_column = self.quote_column(InternalName.ONLINE_STORE_VALUE_COLUMN)
            quoted_entity_columns = (
                [self.quote_column(col.replace('"', "")) for col in f_entity_columns.split(",")]
                if f_entity_columns
                else []
            )
            column_names = ", ".join(
                quoted_entity_columns + [quoted_result_name_column, quoted_value_column]
            )

            current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if not fs_table_exist_flag:
                # feature store table does not exist, create table with the input feature sql
                create_sql = construct_create_table_query(
                    fs_table,
                    f"select {column_names} from ({f_sql})",
                    session=self._session,
                    partition_keys=quoted_result_name_column,
                )
                await self.retry_sql(create_sql)

                if self._session.source_type == SourceType.SNOWFLAKE:
                    await self.retry_sql(
                        f"ALTER TABLE {fs_table} ALTER {quoted_result_name_column} SET DATA TYPE STRING",
                    )

                await self.retry_sql(
                    f"ALTER TABLE {fs_table} ADD COLUMN UPDATED_AT TIMESTAMP",
                )

                await self.retry_sql(
                    sql=f"UPDATE {fs_table} SET UPDATED_AT = to_timestamp('{current_ts}')",
                )
            else:
                # feature store table already exists, insert records with the input feature sql

                value_args = []
                if quoted_entity_columns:
                    value_args.extend([f"b.{element}" for element in quoted_entity_columns])
                value_args += [f"b.{quoted_result_name_column}", f"b.{quoted_value_column}"]
                value_args = ", ".join(value_args)  # type: ignore[assignment]

                # Note: the last condition of "a.{quoted_result_name_column} = '{f_name}'" is
                # required to avoid the ConcurrentAppendException error in delta tables in addition
                # to setting the partition keys.
                keys = " AND ".join(
                    [f"a.{col} = b.{col}" for col in quoted_entity_columns]
                    + [f"a.{quoted_result_name_column} = b.{quoted_result_name_column}"]
                    + [f"a.{quoted_result_name_column} = '{f_name}'"]
                )

                # update or insert feature values for entities that are in entity universe
                merge_sql = f"""
                     merge into {fs_table} a using ({f_sql}) b
                         on {keys}
                         when matched then
                             update set
                               a.{quoted_value_column} = b.{quoted_value_column},
                               a.UPDATED_AT = to_timestamp('{current_ts}')
                         when not matched then
                             insert ({column_names}, UPDATED_AT)
                                 values ({value_args}, to_timestamp('{current_ts}'))
                 """

                await self.retry_sql(sql=merge_sql)

                # remove feature values for entities that are not in entity universe
                remove_values_sql = f"""
                    UPDATE {fs_table} SET {quoted_value_column} = NULL
                    WHERE
                      UPDATED_AT < to_timestamp('{current_ts}')
                      AND {quoted_result_name_column} = '{f_name}'
                    """
                await self.retry_sql(sql=remove_values_sql)
