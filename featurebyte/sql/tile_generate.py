"""
Databricks Tile Generate Job Script
"""

import time
import traceback
from dataclasses import dataclass
from typing import Optional

import dateutil.parser
from bson import ObjectId

from featurebyte.common import date_util
from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.models.system_metrics import SqlQueryMetrics, SqlQueryType, TileComputeMetrics
from featurebyte.models.tile import TileType
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.system_metrics import SystemMetricsService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import QueryMetadata
from featurebyte.session.session_helper import SQL_QUERY_METRICS_LOGGING_THRESHOLD_SECONDS
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_registry import TileRegistry

logger = get_logger(__name__)


# Time to live for temporary tile tables. While this is set as 7 days, in the usual case the tables
# are cleaned up immediately at the end of historical features computation. This is to handle the
# cases where the tables are not cleaned up due to unexpected failures bypassing the cleanup logic.
TEMP_TILE_TABLE_TTL_SECONDS = 86400 * 7


@dataclass
class TileComputeError:
    """
    Tile compute error
    """

    traceback: str


@dataclass
class TileComputeSuccess:
    """
    Tile compute success
    """

    computed_tiles_table_name: str
    tile_sql: str
    tile_compute_metrics: TileComputeMetrics


TileComputeResult = TileComputeSuccess | TileComputeError


class TileGenerate(TileCommon):
    """
    Tile Generate script
    """

    tile_type: TileType
    tile_start_ts_str: Optional[str]
    tile_end_ts_str: Optional[str]
    update_last_run_metadata: Optional[bool]
    tile_registry_service: TileRegistryService
    warehouse_table_service: WarehouseTableService
    deployed_tile_table_service: DeployedTileTableService
    system_metrics_service: SystemMetricsService

    async def execute(self) -> TileComputeMetrics:
        """
        Execute tile generate operation

        Returns
        -------
        TileComputeMetrics
        """
        tile_compute_result = await self.compute_tiles(None)
        assert isinstance(tile_compute_result, TileComputeSuccess)
        try:
            await self.insert_tiles_and_update_metadata(
                computed_tiles_table_name=tile_compute_result.computed_tiles_table_name,
                tile_sql=tile_compute_result.tile_sql,
            )
        finally:
            await self._session.drop_table(
                tile_compute_result.computed_tiles_table_name,
                schema_name=self._session.schema_name,
                database_name=self._session.database_name,
                if_exists=True,
            )
        return tile_compute_result.tile_compute_metrics

    async def compute_tiles(
        self, temp_tile_tables_tag: Optional[str], raise_on_error: bool = True
    ) -> TileComputeResult:
        """
        Compute tiles and store the result in a table for further processing. Caller is responsible
        for cleaning up the table.

        Parameters
        ----------
        temp_tile_tables_tag: Optional[str]
            Tag for temporary tile tables
        raise_on_error: bool
            Whether to raise an error if tile generation fails

        Returns
        -------
        TileComputeResult

        Raises
        ------
        Exception
            If tile generation fails and raise_on_error is True
        """

        # Get the final SQL query
        if self.sql is not None:
            view_cache_seconds = None
            final_sql = self._replace_placeholder_dates(self.sql)
        else:
            assert self.tile_compute_query is not None
            tile_compute_query = self.tile_compute_query
            tic = time.time()
            for table in self.tile_compute_query.prerequisite.tables:
                replaced_query = self._replace_placeholder_dates(table.query.query_str)
                if table.query.query_str != replaced_query:
                    tile_compute_query = tile_compute_query.replace_prerequisite_table_str(
                        name=table.name,
                        new_query_str=replaced_query,
                    )
            view_cache_seconds = time.time() - tic
            final_sql = tile_compute_query.get_combined_query_string()

        tile_sql = self._construct_tile_sql_with_index(final_sql)

        # Compute the tiles
        tic = time.time()
        computed_tiles_table_name = f"__TEMP_TILE_TABLE_{ObjectId()}".upper()
        try:
            query_metadata = QueryMetadata()
            await self.warehouse_table_service.create_table_as_with_session(
                session=self._session,
                feature_store_id=self.feature_store_id,
                tag=temp_tile_tables_tag,
                table_details=computed_tiles_table_name,
                select_expr=tile_sql,
                time_to_live_seconds=TEMP_TILE_TABLE_TTL_SECONDS,
                query_metadata=query_metadata,
            )
            compute_seconds = time.time() - tic
            elapsed = time.time() - tic
            if elapsed > SQL_QUERY_METRICS_LOGGING_THRESHOLD_SECONDS:
                await self.system_metrics_service.create_metrics(
                    SqlQueryMetrics(
                        query=tile_sql,
                        total_seconds=elapsed,
                        query_type=SqlQueryType.TILE_COMPUTE,
                        query_id=query_metadata.query_id,
                    )
                )
            return TileComputeSuccess(
                computed_tiles_table_name=computed_tiles_table_name,
                tile_sql=final_sql,
                tile_compute_metrics=TileComputeMetrics(
                    view_cache_seconds=view_cache_seconds,
                    compute_seconds=compute_seconds,
                ),
            )
        except Exception:
            if raise_on_error:
                raise
            logger.exception(
                "Tile compute failed but raise_on_error=False, proceeding anyway",
                extra={
                    "tile_sql": tile_sql,
                    "tile_id": self.tile_id,
                    "aggregation_id": self.aggregation_id,
                    "feature_store_id": self.feature_store_id,
                },
            )
            return TileComputeError(traceback=traceback.format_exc())

    async def insert_tiles_and_update_metadata(
        self,
        computed_tiles_table_name: str,
        tile_sql: str,
    ) -> None:
        """
        Update tile tables with the computed tiles

        Parameters
        ----------
        computed_tiles_table_name: str
            Name of the table containing the computed tiles
        tile_sql: str
            SQL query used to compute the tiles
        """
        await TileRegistry(
            session=self._session,
            sql=tile_sql,
            computed_tiles_table_name=computed_tiles_table_name,
            time_modulo_frequency_second=self.time_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_id=self.tile_id,
            aggregation_id=self.aggregation_id,
            feature_store_id=self.feature_store_id,
            tile_registry_service=self.tile_registry_service,
        ).execute()

        entity_insert_cols = []
        entity_filter_cols = []
        for element in self.entity_column_names:
            quote_element = self.quote_column(element.strip())
            entity_insert_cols.append(f"b.{quote_element}")
            entity_filter_cols.append(
                self.quote_column_null_aware_equal(f"a.{quote_element}", f"b.{quote_element}")
            )

        entity_insert_cols_str = ",".join(entity_insert_cols)
        entity_filter_cols_str = " AND ".join(entity_filter_cols)

        value_insert_cols = []
        value_update_cols = []
        for element in self.value_column_names:
            element = element.strip()
            value_insert_cols.append("b." + element)
            value_update_cols.append("a." + element + " = b." + element)

        value_insert_cols_str = ",".join(value_insert_cols)
        value_update_cols_str = ",".join(value_update_cols)

        # insert new records and update existing records
        logger.debug("merging into tile table", extra={"tile_id": self.tile_id})
        value_column_names_str = ",".join(self.value_column_names)
        if self.entity_column_names:
            on_condition_str = f"a.INDEX = b.INDEX AND {entity_filter_cols_str}"
            insert_str = (
                f"INDEX, {self.entity_column_names_str}, {value_column_names_str}, CREATED_AT"
            )
            values_str = (
                f"b.INDEX, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp()"
            )
        else:
            on_condition_str = "a.INDEX = b.INDEX"
            insert_str = f"INDEX, {value_column_names_str}, CREATED_AT"
            values_str = f"b.INDEX, {value_insert_cols_str}, current_timestamp()"

        merge_sql = f"""
            merge into {self.tile_id} a using {computed_tiles_table_name} b
                on {on_condition_str}
                when matched then
                    update set a.created_at = current_timestamp(), {value_update_cols_str}
                when not matched then
                    insert ({insert_str})
                        values ({values_str})
        """
        await self._session.retry_sql(sql=merge_sql)

        if self.tile_end_ts_str is not None and self.update_last_run_metadata:
            tile_end_date = dateutil.parser.isoparse(self.tile_end_ts_str)
            ind_value = date_util.timestamp_utc_to_tile_index(
                tile_end_date,
                self.time_modulo_frequency_second,
                self.blind_spot_second,
                self.frequency_minute,
            )

            logger.debug(
                "Updating last run metadata for tile",
                extra={"tile_end_ts_str": self.tile_end_ts_str, "ind_value": ind_value},
            )

            if self.deployed_tile_table_id is None:
                await self.tile_registry_service.update_last_run_metadata(
                    tile_id=self.tile_id,
                    aggregation_id=self.aggregation_id,
                    tile_type=self.tile_type,
                    tile_index=ind_value,
                    tile_end_date=tile_end_date,
                )
            else:
                await self.deployed_tile_table_service.update_last_run_metadata(
                    document_id=self.deployed_tile_table_id,
                    tile_type=self.tile_type,
                    tile_index=ind_value,
                    tile_end_date=tile_end_date,
                )

    def _construct_tile_sql_with_index(self, tile_sql: str) -> str:
        if self.entity_column_names:
            entity_and_value_column_names_str = (
                f"{self.entity_column_names_str}, {self.value_column_names_str}"
            )
        else:
            entity_and_value_column_names_str = self.value_column_names_str

        tile_sql = f"""
            select
                index,
                {entity_and_value_column_names_str},
                current_timestamp() as created_at
            from ({tile_sql})
        """
        return tile_sql

    def _replace_placeholder_dates(self, query: str) -> str:
        if self.tile_start_ts_str is not None:
            query = query.replace(
                f"{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}", f"'{self.tile_start_ts_str}'"
            )
        if self.tile_end_ts_str is not None:
            query = query.replace(
                f"{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}", f"'{self.tile_end_ts_str}'"
            )
        return query
