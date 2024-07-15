"""
TileManagerService class
"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, List, Optional, Tuple

import time

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.models.tile import TileScheduledJobParameters, TileSpec, TileType
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.online_store_table_version import OnlineStoreTableVersionService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.service.tile_scheduler import TileSchedulerService
from featurebyte.session.base import BaseSession
from featurebyte.session.session_helper import run_coroutines
from featurebyte.sql.tile_generate import TileGenerate
from featurebyte.sql.tile_generate_entity_tracking import TileGenerateEntityTracking
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
    ):
        self.online_store_table_version_service = online_store_table_version_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.tile_scheduler_service = tile_scheduler_service
        self.tile_registry_service = tile_registry_service
        self.feature_service = feature_service

    async def generate_tiles_on_demand(
        self,
        session: BaseSession,
        tile_inputs: List[Tuple[TileSpec, str]],
        progress_callback: Optional[Callable[[int, str], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        progress_callback: Optional[Callable[[int, str | None], Coroutine[Any, Any, None]]]
            Optional progress callback function
        """
        num_jobs = len(tile_inputs)
        processed = 0

        async def _progress_callback() -> None:
            nonlocal processed
            processed += 1
            if progress_callback:
                pct = int(100 * processed / num_jobs)
                await progress_callback(pct, f"Computed {processed} out of {num_jobs} tiles")

        if progress_callback:
            await progress_callback(0, "Computing tiles on demand")
        coroutines = []
        for tile_spec, entity_table in tile_inputs:
            coroutines.append(
                self._generate_tiles_on_demand_for_tile_spec(
                    session=session,
                    tile_spec=tile_spec,
                    entity_table=entity_table,
                    progress_callback=_progress_callback,
                )
            )
        await run_coroutines(coroutines)

    async def _generate_tiles_on_demand_for_tile_spec(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        entity_table: str,
        progress_callback: Optional[Callable[[], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        tic = time.time()
        session = await session.clone_if_not_threadsafe()
        await self.generate_tiles(
            session=session,
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            start_ts_str=None,
            end_ts_str=None,
        )
        logger.debug(
            "Done generating tiles",
            extra={"tile_id": tile_spec.tile_id, "duration": time.time() - tic},
        )

        tic = time.time()
        await self.update_tile_entity_tracker(
            session=session, tile_spec=tile_spec, temp_entity_table=entity_table
        )
        logger.debug(
            "Done update_tile_entity_tracker",
            extra={"tile_id": tile_spec.tile_id, "duration": time.time() - tic},
        )

        if progress_callback:
            await progress_callback()

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

    async def generate_tiles(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        last_tile_start_ts_str: Optional[str] = None,
    ) -> None:
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
        last_tile_start_ts_str: str
            start date string of last tile used to update the tile_registry table
        """

        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        tile_generate_ins = TileGenerate(
            session=session,
            feature_store_id=tile_spec.feature_store_id,
            tile_id=tile_spec.tile_id,
            time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            sql=tile_sql,
            entity_column_names=tile_spec.entity_column_names,
            value_column_names=tile_spec.value_column_names,
            value_column_types=tile_spec.value_column_types,
            tile_type=tile_type,
            last_tile_start_str=last_tile_start_ts_str,
            aggregation_id=tile_spec.aggregation_id,
            tile_registry_service=self.tile_registry_service,
        )
        await tile_generate_ins.execute()

    async def update_tile_entity_tracker(
        self, session: BaseSession, tile_spec: TileSpec, temp_entity_table: str
    ) -> str:
        """
        Update <tile_id>_entity_tracker table for last_tile_start_date

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        tile_spec: TileSpec
            the input TileSpec
        temp_entity_table: str
            temporary entity table to be merged into <tile_id>_entity_tracker

        Returns
        -------
            spark job run detail
        """
        if tile_spec.category_column_name is None:
            entity_column_names = tile_spec.entity_column_names
        else:
            entity_column_names = [
                c for c in tile_spec.entity_column_names if c != tile_spec.category_column_name
            ]

        tile_entity_tracking_ins = TileGenerateEntityTracking(
            session=session,
            entity_tracker_table_name=tile_spec.entity_tracker_table_name,
            entity_column_names=entity_column_names,
            entity_table=temp_entity_table,
        )

        await tile_entity_tracking_ins.execute()

        return tile_entity_tracking_ins.json()

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

            return parameters.json()

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
