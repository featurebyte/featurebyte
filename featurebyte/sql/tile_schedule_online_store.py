"""
Tile Generate online store Job Script
"""

from typing import List, Optional

import textwrap
from datetime import datetime

from pydantic import Field

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.models.online_store_table_version import OnlineStoreTableVersion
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.sql.base import BaseSqlModel
from featurebyte.sql.common import register_temporary_physical_table

logger = get_logger(__name__)


class TileScheduleOnlineStore(BaseSqlModel):
    """
    Tile Schedule Online Store script
    """

    aggregation_id: str
    job_schedule_ts_str: str
    aggregation_result_name: Optional[str] = Field(default=None)
    online_store_table_version_service: OnlineStoreTableVersionService
    online_store_compute_query_service: OnlineStoreComputeQueryService

    class Config:
        """
        Config class to allow services to be passed in as arguments
        """

        arbitrary_types_allowed = True

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
            current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # get current version
            current_version = await self.online_store_table_version_service.get_version(f_name)
            if current_version is None:
                next_version = 0
            else:
                next_version = current_version + 1

            if not fs_table_exist_flag:
                # feature store table does not exist, create table with the input feature sql
                query = "SELECT"
                if quoted_entity_columns:
                    query += f" {', '.join(quoted_entity_columns)},"
                col_type = compute_query.result_type
                # Apply cast to ensure the types are not overly restrictive and fail subsequent
                # inserts. NOTE: Longer term we should move the casting into f_sql and avoid manual
                # formatting, but we need sqlglot>=17.0.0 to properly handle complex types such as
                # MAP<STRING, DOUBLE> in spark.
                query += textwrap.dedent(
                    f"""
                      CAST({quoted_result_name_column} AS STRING) AS {quoted_result_name_column},
                      CAST({quoted_value_column} AS {col_type}) AS {quoted_value_column},
                      CAST({next_version} AS INT) AS {quoted_version_column},
                      to_timestamp('{current_ts}') AS UPDATED_AT
                    FROM ({f_sql})
                    """
                )
                await self._session.create_table_as(
                    table_details=fs_table,
                    select_expr=query,
                    partition_keys=[InternalName.ONLINE_STORE_RESULT_NAME_COLUMN],
                )
            else:
                # feature store table already exists, insert records with the input feature sql
                column_names = ", ".join(
                    quoted_entity_columns + [quoted_result_name_column, quoted_value_column]
                )
                async with register_temporary_physical_table(self._session, f_sql) as temp_table:
                    insert_query = textwrap.dedent(
                        f"""
                        INSERT INTO {fs_table} ({column_names}, {quoted_version_column}, UPDATED_AT)
                        SELECT {column_names}, {next_version}, to_timestamp('{current_ts}')
                        FROM {temp_table}
                        """
                    )
                    await self._session.execute_query_long_running(insert_query)
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
