"""
Tile Generate online store Job Script
"""
from typing import Any, Union

import textwrap
from datetime import datetime

import pandas as pd
from pydantic import Field, PrivateAttr

from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.models.online_store_table_version import OnlineStoreTableVersion
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
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

    _online_store_table_version_service: OnlineStoreTableVersionService = PrivateAttr()

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
        online_store_table_version_service = kwargs.pop("online_store_table_version_service")
        self._online_store_table_version_service = online_store_table_version_service
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
            quoted_version_column = self.quote_column(InternalName.ONLINE_STORE_VERSION_COLUMN)
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
                    f"select {column_names}, 0 AS {quoted_version_column} from ({f_sql})",
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

                # update online store table version in mongo
                version_model = OnlineStoreTableVersion(
                    online_store_table_name=fs_table,
                    aggregation_result_name=f_name,
                    version=0,
                )
                await self._online_store_table_version_service.create_document(version_model)
            else:
                # feature store table already exists, insert records with the input feature sql

                # get current version
                current_version = await self._online_store_table_version_service.get_version(f_name)
                if current_version is None:
                    next_version = 0
                else:
                    next_version = current_version + 1
                # current_version = (
                #     await self._session.execute_query(
                #         f"""
                #         SELECT MAX({quoted_version_column}) AS OUT
                #         FROM {fs_table}
                #         WHERE {quoted_result_name_column} = '{f_name}'
                #         """
                #     )
                # ).iloc[0]["OUT"]
                # if pd.isna(current_version):
                #     next_version = 0
                # else:
                #     next_version = current_version + 1

                insert_query = textwrap.dedent(
                    f"""
                    INSERT INTO {fs_table} ({column_names}, {quoted_version_column}, UPDATED_AT)
                    SELECT {column_names}, {next_version}, to_timestamp('{current_ts}')
                    FROM ({f_sql})
                    """
                )
                await self._session.execute_query(insert_query)
                logger.debug(
                    "Done inserting to online store",
                    extra={"fs_table": fs_table, "result_name": f_name, "version": next_version},
                )

                # update online store table version in mongo
                if current_version is None:
                    version_model = OnlineStoreTableVersion(
                        online_store_table_name=fs_table,
                        aggregation_result_name=f_name,
                        version=next_version,
                    )
                    await self._online_store_table_version_service.create_document(version_model)
                else:
                    await self._online_store_table_version_service.update_version(
                        f_name, next_version
                    )
