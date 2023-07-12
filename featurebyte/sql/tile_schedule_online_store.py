"""
Tile Generate online store Job Script
"""
from typing import List, Optional, Union

import textwrap
from datetime import datetime

import pandas as pd
from pydantic import Field

from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.models.online_store_table_version import OnlineStoreTableVersion
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.sql.base import BaseSqlModel
from featurebyte.sql.common import construct_create_table_query, retry_sql

logger = get_logger(__name__)


class TileScheduleOnlineStore(BaseSqlModel):
    """
    Tile Schedule Online Store script
    """

    aggregation_id: str
    job_schedule_ts_str: str
    retry_num: int = Field(default=10)
    aggregation_result_name: Optional[str] = Field(default=None)
    online_store_table_version_service: OnlineStoreTableVersionService
    online_store_compute_query_service: OnlineStoreComputeQueryService

    class Config:
        """
        Config class to allow services to be passed in as arguments
        """

        arbitrary_types_allowed = True

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

    async def execute(self) -> None:
        """
        Execute tile schedule online store operation
        """
        # pylint: disable=too-many-locals,too-many-statements
        compute_queries = await self._retrieve_online_store_compute_queries()

        for compute_query in compute_queries:
            f_name = compute_query.result_name
            f_sql = compute_query.sql
            fs_table = compute_query.table_name
            f_entity_columns = compute_query.serving_names
            f_sql = f_sql.replace(
                "__FB_POINT_IN_TIME_SQL_PLACEHOLDER", "'" + self.job_schedule_ts_str + "'"
            )

            logger.debug(
                "Populating online store table",
                extra={"aggregation_result_name": f_name, "online_store_table_name": fs_table},
            )

            # check if feature store table exists
            fs_table_exist_flag = await self.table_exists(fs_table)

            quoted_result_name_column = self.quote_column(
                InternalName.ONLINE_STORE_RESULT_NAME_COLUMN
            )
            quoted_value_column = self.quote_column(InternalName.ONLINE_STORE_VALUE_COLUMN)
            quoted_version_column = self.quote_column(InternalName.ONLINE_STORE_VERSION_COLUMN)
            quoted_entity_columns = (
                [self.quote_column(col) for col in f_entity_columns] if f_entity_columns else []
            )
            column_names = ", ".join(
                quoted_entity_columns + [quoted_result_name_column, quoted_value_column]
            )

            current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # get current version
            current_version = await self.online_store_table_version_service.get_version(f_name)
            if current_version is None:
                next_version = 0
            else:
                next_version = current_version + 1

            if not fs_table_exist_flag:
                # feature store table does not exist, create table with the input feature sql
                query = textwrap.dedent(
                    f"""
                    SELECT
                      {column_names},
                      CAST({next_version} AS INT) AS {quoted_version_column}
                    FROM ({f_sql})
                    """
                ).strip()
                create_sql = construct_create_table_query(
                    fs_table,
                    query,
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
                await self.online_store_table_version_service.create_document(version_model)
            else:
                await self.online_store_table_version_service.update_version(f_name, next_version)

    async def _retrieve_online_store_compute_queries(self) -> List[OnlineStoreComputeQueryModel]:
        if self.aggregation_result_name is not None:
            # Retrieve compute queries for a specific result name (e.g. sum_30d)
            iterator = self.online_store_compute_query_service.list_by_result_names(
                [self.aggregation_result_name]
            )
        else:
            # Retrieve all compute queries associated with an aggregation_id (e.g. sum_1d, sum_7d,
            # sum_30d, etc)
            iterator = self.online_store_compute_query_service.list_by_aggregation_id(
                self.aggregation_id
            )
        out = []
        async for doc in iterator:
            out.append(doc)
        return out
