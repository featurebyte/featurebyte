"""
TileManagerService class
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, List, Optional

from redis import Redis

from featurebyte.common.progress import ProgressCallbackType
from featurebyte.exception import DocumentNotFoundError
from featurebyte.logging import get_logger
from featurebyte.models import FeatureStoreModel
from featurebyte.models.system_metrics import TileComputeMetrics
from featurebyte.models.tile import (
    OnDemandTileComputeResult,
    OnDemandTileSpec,
    OnDemandTileTable,
    TileScheduledJobParameters,
    TileSpec,
    TileType,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.service.tile_scheduler import TileSchedulerService
from featurebyte.service.warehouse_table_service import WarehouseTableService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import run_coroutines
from featurebyte.sql.tile_generate import TileComputeResult, TileGenerate
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore

logger = get_logger(__name__)


class TileManagerService:
    """
    TileManagerService is responsible for materialization of tiles in the data warehouse and
    scheduling of periodic tile jobs
    """

    def __init__(
        self,
        online_store_table_version_service: OnlineStoreTableVersionService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        tile_scheduler_service: TileSchedulerService,
        tile_registry_service: TileRegistryService,
        feature_service: FeatureService,
        feature_store_service: FeatureStoreService,
        warehouse_table_service: WarehouseTableService,
        redis: Redis[Any],
    ):
        self.online_store_table_version_service = online_store_table_version_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.tile_scheduler_service = tile_scheduler_service
        self.tile_registry_service = tile_registry_service
        self.feature_service = feature_service
        self.feature_store_service = feature_store_service
        self.warehouse_table_service = warehouse_table_service
        self.redis = redis

    async def generate_tiles_on_demand(
        self,
        session: BaseSession,
        tile_inputs: List[OnDemandTileSpec],
        temp_tile_tables_tag: str,
        progress_callback: Optional[Callable[[int, str], Coroutine[Any, Any, None]]] = None,
    ) -> OnDemandTileComputeResult:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        temp_tile_tables_tag: str
            Tag to use when creating temporary tile tables
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function

        Returns
        -------
        OnDemandTileComputeResult
        """
        _compute_tiles_progress_callback = self._on_demand_tiles_progress_callback(
            tile_inputs=tile_inputs,
            progress_callback=progress_callback,
        )

        if progress_callback:
            await _compute_tiles_progress_callback()

        feature_store_id = tile_inputs[0].tile_spec.feature_store_id
        max_query_currency = None
        if feature_store_id:
            try:
                feature_store: FeatureStoreModel = await self.feature_store_service.get_document(
                    document_id=feature_store_id
                )
                max_query_currency = feature_store.max_query_concurrency
            except DocumentNotFoundError:
                pass

        coroutines = []
        for on_demand_tile_spec in tile_inputs:
            coroutines.append(
                self._on_demand_compute_tiles(
                    session=session,
                    on_demand_tile_spec=on_demand_tile_spec,
                    temp_tile_tables_tag=temp_tile_tables_tag,
                    progress_callback=_compute_tiles_progress_callback,
                )
            )
        tile_compute_results: List[TileComputeResult] = await run_coroutines(
            coroutines, self.redis, str(feature_store_id), max_query_currency
        )

        on_demand_tile_tables = []
        for on_demand_tile_spec, tile_compute_result in zip(tile_inputs, tile_compute_results):
            # Do not update permanent tile tables but use the computed tile table directly
            # in feature query.
            for grouping in on_demand_tile_spec.tile_table_groupings:
                on_demand_tile_tables.append(
                    OnDemandTileTable(
                        tile_table_id=grouping.tile_id,
                        on_demand_table_name=tile_compute_result.computed_tiles_table_name,
                    )
                )

        # Aggregate metrics from the tasks
        tile_compute_metrics_list: List[TileComputeMetrics] = [
            result.tile_compute_metrics for result in tile_compute_results
        ]
        view_cache_seconds = 0.0
        compute_seconds = 0.0
        for metrics in tile_compute_metrics_list:
            if metrics.view_cache_seconds is not None:
                view_cache_seconds += metrics.view_cache_seconds
            if metrics.compute_seconds is not None:
                compute_seconds += metrics.compute_seconds
        metrics = TileComputeMetrics(
            view_cache_seconds=view_cache_seconds,
            compute_seconds=compute_seconds,
        )

        return OnDemandTileComputeResult(
            tile_compute_metrics=metrics,
            on_demand_tile_tables=on_demand_tile_tables,
        )

    @classmethod
    def _on_demand_tiles_progress_callback(
        cls,
        tile_inputs: List[OnDemandTileSpec],
        progress_callback: Optional[ProgressCallbackType] = None,
    ) -> ProgressCallbackType:
        num_tile_tables_to_compute = len(tile_inputs)
        num_tile_tables_computed = -1

        async def _compute_tiles_progress_callback() -> None:
            nonlocal num_tile_tables_computed
            num_tile_tables_computed += 1
            if progress_callback:
                pct = int(100 * num_tile_tables_computed / num_tile_tables_to_compute)
                await progress_callback(
                    pct,
                    f"Computed {num_tile_tables_computed} out of {num_tile_tables_to_compute} tile tables",
                )

        return _compute_tiles_progress_callback

    async def _on_demand_compute_tiles(
        self,
        session: BaseSession,
        on_demand_tile_spec: OnDemandTileSpec,
        temp_tile_tables_tag: str,
        progress_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
    ) -> TileComputeResult:
        session = await session.clone_if_not_threadsafe()
        tile_generate_obj = self._get_tile_generate_object_from_tile_spec(
            session=session,
            tile_spec=on_demand_tile_spec.tile_spec,
            tile_type=TileType.OFFLINE,
            start_ts_str=None,
            end_ts_str=None,
        )
        result = await tile_generate_obj.compute_tiles(temp_tile_tables_tag=temp_tile_tables_tag)
        if progress_callback:
            await progress_callback()
        return result

    async def tile_job_exists(self, tile_spec: TileSpec) -> bool:
        """
        Get existing tile jobs for the given tile_spec

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec

        Returns
        -------
            whether the tile jobs already exist
        """
        job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"
        return await self.tile_scheduler_service.get_job_details(job_id=job_id) is not None

    async def populate_feature_store(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        job_schedule_ts_str: str,
        aggregation_result_name: str,
    ) -> None:
        """
        Populate feature store with the given tile_spec and timestamp string

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        tile_spec: TileSpec
            the input TileSpec
        job_schedule_ts_str: str
            timestamp string of the job schedule
        aggregation_result_name: str
            aggregation result name to populate
        """
        executor = TileScheduleOnlineStore(
            session=session,
            aggregation_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
            online_store_table_version_service=self.online_store_table_version_service,
            online_store_compute_query_service=self.online_store_compute_query_service,
            aggregation_result_name=aggregation_result_name,
        )
        await executor.execute()

    def _get_tile_generate_object_from_tile_spec(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        update_last_run_metadata: bool = False,
    ) -> TileGenerate:
        return TileGenerate(
            session=session,
            feature_store_id=tile_spec.feature_store_id,
            tile_id=tile_spec.tile_id,
            time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            sql=tile_spec.tile_sql,
            tile_compute_query=tile_spec.tile_compute_query,
            entity_column_names=tile_spec.entity_column_names,
            value_column_names=tile_spec.value_column_names,
            value_column_types=tile_spec.value_column_types,
            tile_type=tile_type,
            tile_start_ts_str=start_ts_str,
            tile_end_ts_str=end_ts_str,
            update_last_run_metadata=update_last_run_metadata,
            aggregation_id=tile_spec.aggregation_id,
            tile_registry_service=self.tile_registry_service,
            warehouse_table_service=self.warehouse_table_service,
        )

    async def generate_tiles(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        update_last_run_metadata: bool = False,
    ) -> TileComputeMetrics:
        """
        Manually trigger tile generation

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            tile type. ONLINE or OFFLINE
        start_ts_str: str
            start_timestamp of tile. ie. 2022-06-20 15:00:00
        end_ts_str: str
            end_timestamp of tile. ie. 2022-06-21 15:00:00
        update_last_run_metadata: bool
            whether to update last run metadata (intended to be set when enabling deployment and
            when running scheduled tile jobs)

        Returns
        -------
        TileComputeMetrics
        """
        tile_generate_ins = self._get_tile_generate_object_from_tile_spec(
            session=session,
            tile_spec=tile_spec,
            tile_type=tile_type,
            start_ts_str=start_ts_str,
            end_ts_str=end_ts_str,
            update_last_run_metadata=update_last_run_metadata,
        )
        return await tile_generate_ins.execute()

    async def schedule_online_tiles(
        self,
        tile_spec: TileSpec,
        monitor_periods: int = 10,
    ) -> Optional[str]:
        """
        Schedule online tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        monitor_periods: int
            number of tile periods to monitor and re-generate. Default is 10

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """
        sql = await self._schedule_tiles_custom(
            tile_spec=tile_spec,
            tile_type=TileType.ONLINE,
            monitor_periods=monitor_periods,
        )

        return sql

    async def schedule_offline_tiles(
        self,
        tile_spec: TileSpec,
        offline_minutes: int = 1440,
    ) -> Optional[str]:
        """
        Schedule offline tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        offline_minutes: int
            offline tile lookback minutes to monitor and re-generate. Default is 1440

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """

        sql = await self._schedule_tiles_custom(
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            offline_minutes=offline_minutes,
        )

        return sql

    async def _schedule_tiles_custom(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        offline_minutes: int = 1440,
        monitor_periods: int = 10,
    ) -> Optional[str]:
        """
        Common tile schedule method

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            ONLINE or OFFLINE
        offline_minutes: int
            offline tile lookback minutes
        monitor_periods: int
            online tile lookback period

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """

        logger.info(f"Scheduling {tile_type} tile job for {tile_spec.aggregation_id}")
        job_id = f"{tile_type}_{tile_spec.aggregation_id}"

        assert tile_spec.feature_store_id is not None
        exist_job = await self.tile_scheduler_service.get_job_details(job_id=job_id)
        if not exist_job:
            logger.info(f"Creating new job {job_id}")
            parameters = TileScheduledJobParameters(
                feature_store_id=tile_spec.feature_store_id,
                tile_id=tile_spec.tile_id,
                time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                blind_spot_second=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
                sql=tile_spec.tile_sql,
                tile_compute_query=tile_spec.tile_compute_query,
                entity_column_names=tile_spec.entity_column_names,
                value_column_names=tile_spec.value_column_names,
                value_column_types=tile_spec.value_column_types,
                tile_type=tile_type,
                offline_period_minute=offline_minutes,
                monitor_periods=monitor_periods,
                aggregation_id=tile_spec.aggregation_id,
            )
            interval_seconds = (
                tile_spec.frequency_minute * 60
                if tile_type == TileType.ONLINE
                else offline_minutes * 60
            )
            await self.tile_scheduler_service.start_job_with_interval(
                job_id=job_id,
                interval_seconds=interval_seconds,
                time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                feature_store_id=tile_spec.feature_store_id,
                parameters=parameters,
            )

            return str(parameters.model_dump_json())

        return None

    async def remove_tile_jobs(self, aggregation_id: str) -> None:
        """
        Remove tiles

        Parameters
        ----------
        aggregation_id: str
            Aggregation id that identifies the tile job to be removed
        """
        async for _ in self.feature_service.list_documents_as_dict_iterator(
            query_filter={
                "aggregation_ids": aggregation_id,
                "online_enabled": True,
            }
        ):
            break
        else:
            # Only disable the tile job if the aggregation_id is not referenced by any currently
            # online enabled features
            logger.info("Stopping job with custom scheduler")
            for t_type in [TileType.ONLINE, TileType.OFFLINE]:
                await self.tile_scheduler_service.stop_job(job_id=f"{t_type}_{aggregation_id}")
