"""
FeatureManagerService class
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from bson import ObjectId

from featurebyte.common import date_util
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import SourceType
from featurebyte.exception import DocumentNotFoundError
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.logging import get_logger
from featurebyte.models import FeatureModel
from featurebyte.models.deployed_tile_table import DeployedTileTableModel
from featurebyte.models.deployment import FeastIntegrationSettings
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_store_cleanup_scheduler import OnlineStoreCleanupSchedulerService
from featurebyte.service.online_store_compute_query_service import OnlineStoreComputeQueryService
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.session.base import BaseSession
from featurebyte.session.databricks_unity import DatabricksUnitySession

logger = get_logger(__name__)


def get_previous_job_datetime(
    input_dt: datetime, frequency_minutes: int, time_modulo_frequency_seconds: int
) -> datetime:
    """
    Calculate the expected current job datetime give input datetime, frequency_minutes and
    time_modulo_frequency_seconds.

    Parameters
    ----------
    input_dt: datetime
        input datetime
    frequency_minutes: int
        frequency in minutes
    time_modulo_frequency_seconds: int
        time_modulo_frequency in seconds

    Returns
    -------
    datetime
    """
    next_job_time = get_next_job_datetime(
        input_dt=input_dt,
        frequency_minutes=frequency_minutes,
        time_modulo_frequency_seconds=time_modulo_frequency_seconds,
    )
    previous_job_time = next_job_time - timedelta(minutes=frequency_minutes)
    return previous_job_time


class FeatureManagerService:
    """
    FeatureManagerService is responsible for orchestrating the materialization of features and tiles
    when a feature is online enabled or disabled.
    """

    def __init__(
        self,
        catalog_id: ObjectId,
        tile_manager_service: TileManagerService,
        tile_registry_service: TileRegistryService,
        online_store_compute_query_service: OnlineStoreComputeQueryService,
        online_store_cleanup_scheduler_service: OnlineStoreCleanupSchedulerService,
        feature_service: FeatureService,
        deployed_tile_table_service: DeployedTileTableService,
    ):
        self.catalog_id = catalog_id
        self.tile_manager_service = tile_manager_service
        self.tile_registry_service = tile_registry_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.online_store_cleanup_scheduler_service = online_store_cleanup_scheduler_service
        self.feature_service = feature_service
        self.deployed_tile_table_service = deployed_tile_table_service

    async def online_enable(
        self,
        session: BaseSession,
        feature_spec: ExtendedFeatureModel,
        schedule_time: Optional[datetime] = None,
    ) -> None:
        """
        Task to update data warehouse when the feature is online enabled (e.g. schedule tile jobs)

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        feature_spec: ExtendedFeatureModel
            Instance of ExtendedFeatureModel
        schedule_time: Optional[datetime]
            the moment of scheduling the job
        """
        if (
            FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED
            and session.source_type == SourceType.DATABRICKS_UNITY
        ):
            # Register Databricks UDF for on-demand feature
            assert isinstance(session, DatabricksUnitySession)
            await self.may_register_databricks_udf_for_on_demand_feature(session, feature_spec)

        if schedule_time is None:
            schedule_time = datetime.utcnow()

        logger.info(
            "Online enabling a feature",
            extra={"feature_name": feature_spec.name, "schedule_time": schedule_time},
        )

        # create online store compute queries for the unscheduled result names
        deployed_tile_table_info = (
            await self.deployed_tile_table_service.get_deployed_tile_table_info(
                set(feature_spec.aggregation_ids),
            )
        )

        # backfill historical tiles if required
        aggregation_id_to_tile_spec = {}
        for tile_spec in feature_spec.tile_specs:
            aggregation_id_to_tile_spec[tile_spec.aggregation_id] = tile_spec

        deployed_tile_tables_to_be_scheduled = []
        for deployed_tile_table in deployed_tile_table_info.deployed_tile_tables:
            job_exists = await self.tile_manager_service.tile_job_exists(
                info=deployed_tile_table.id
            )
            if not job_exists:
                deployed_tile_tables_to_be_scheduled.append(deployed_tile_table)
            await self._backfill_tiles(
                session=session,
                deployed_tile_table=deployed_tile_table,
                tile_specs=feature_spec.tile_specs,
                schedule_time=schedule_time,
            )

        # enable tile generation with scheduled jobs
        for deployed_tile_table in deployed_tile_tables_to_be_scheduled:
            # enable online tiles scheduled job
            tile_spec = deployed_tile_table.to_tile_spec()
            await self.tile_manager_service.schedule_online_tiles(
                tile_spec=tile_spec,
                deployed_tile_table_id=deployed_tile_table.id,
            )
            logger.debug(f"Done schedule_online_tiles for {tile_spec.aggregation_id}")

            # enable offline tiles scheduled job
            await self.tile_manager_service.schedule_offline_tiles(
                tile_spec=tile_spec,
                deployed_tile_table_id=deployed_tile_table.id,
            )
            logger.debug(f"Done schedule_offline_tiles for {tile_spec.aggregation_id}")

    async def _backfill_tiles(
        self,
        session: BaseSession,
        deployed_tile_table: DeployedTileTableModel,
        tile_specs: List[TileSpec],
        schedule_time: datetime,
    ) -> None:
        """
        Backfill tiles required to populate internal online store

        This will determine the tiles that need to be backfilled using information stored in the
        tile registry collection. Tiles between backfill_start_date and last_run_tile_end_date can
        be assumed to be available because of previous backfills and on going scheduled tile tasks.
        See an example below.

        Feature: Amount Sum over 48 hours

                 <-------------------------------- 48h ------------------------------->
                                            (tiles available)
                                      # # # # # # # # # # # # # # #
        ---------|-------------------|----------------------------|-------------------|--> (time)
                 ^                   ^                            ^                   ^
             start_ts        backfill_start_date      last_run_tile_end_date       end_ts
                 <------------------->                            <------------------->
                    required compute                                 required compute
           (start_ts_1)        (end_ts_1)                 (start_ts_2)           (end_ts_2)

        Notes:

        * start_ts: start timestamp of the tiles that need to be computed. This is determined by
          end_ts and the largest feature derivation window of the feature.

        * end_ts: end timestamp of the tiles that need to be computed. This is determined by the
          feature job settings and the deployment time.

        * backfill_start_date: start timestamp of the tile that was last backfilled. This is updated
          by tile backfill process during deployment.

        * last_run_tile_end_date: end timestamp of the tile that was last computed in the scheduled
          tile task. This is updated by scheduled tile task regularly.

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        deployed_tile_table: DeployedTileTableModel
            DeployedTileTableModel instance
        schedule_time: datetime
            The moment of enabling the feature
        tile_specs: List[TileSpec]
            TileSpec objects for the feature. Used to determine the largest feature derivation
            window.
        """
        # Derive the tile end date based on expected previous job time
        job_schedule_ts = get_previous_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=deployed_tile_table.frequency_minute,
            time_modulo_frequency_seconds=deployed_tile_table.time_modulo_frequency_second,
        )
        end_ts = job_schedule_ts - timedelta(seconds=deployed_tile_table.blind_spot_second)

        # Determine the earliest tiles required to compute the features for offline store. This is
        # determined by the largest feature derivation window.
        max_window_seconds: Optional[int] = None
        deployed_tile_table_aggregation_ids = set(deployed_tile_table.aggregation_ids)
        tile_specs = [
            tile_spec
            for tile_spec in tile_specs
            if tile_spec.aggregation_id in deployed_tile_table_aggregation_ids
        ]
        for tile_spec in tile_specs:
            for window in tile_spec.windows:
                if window is None:
                    max_window_seconds = None
                    break
                window_seconds = parse_duration_string(window)
                if tile_spec.offset is not None:
                    window_seconds += parse_duration_string(tile_spec.offset)
                if max_window_seconds is None or window_seconds > max_window_seconds:
                    max_window_seconds = window_seconds

        if max_window_seconds is None:
            # Need to backfill all the way back for features without a bounded window
            start_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        else:
            # For features with a bounded window, backfill only the required tiles
            start_ts = end_ts - timedelta(seconds=max_window_seconds)

        start_ts_1: Optional[datetime] = None
        end_ts_1: Optional[datetime] = None
        start_ts_2: Optional[datetime] = None
        end_ts_2: Optional[datetime] = None

        if deployed_tile_table.backfill_metadata is not None:
            if start_ts.replace(tzinfo=None) < deployed_tile_table.backfill_metadata.start_date:
                # Need to compute tiles up to previous backfill start date
                start_ts_1 = start_ts
                end_ts_1 = deployed_tile_table.backfill_metadata.start_date
            if deployed_tile_table.last_run_metadata_offline is None:
                # Note: unlikely to happen as there should already be a tile job running, but we
                # still need to handle this case in case of bad state / error
                start_ts_2 = deployed_tile_table.backfill_metadata.start_date
                end_ts_2 = end_ts
            elif deployed_tile_table.last_run_metadata_offline.tile_end_date < end_ts.replace(
                tzinfo=None
            ):
                # Need to compute tiles from last run tile end date to end_ts
                start_ts_2 = deployed_tile_table.last_run_metadata_offline.tile_end_date
                end_ts_2 = end_ts
            to_init_backfill_metadata = False
        else:
            # Need to compute tiles up to end_ts
            start_ts_2 = start_ts
            end_ts_2 = end_ts
            to_init_backfill_metadata = True

        logger.info(
            "Determined tile start and end timestamps for online enabling",
            extra={
                "start_ts_1": str(start_ts_1),
                "end_ts_1": str(end_ts_1),
                "start_ts_2": str(start_ts_2),
                "end_ts_2": str(end_ts_2),
            },
        )

        if start_ts_1 is not None and end_ts_1 is not None:
            await self._backfill_tiles_with_start_end(
                session=session,
                deployed_tile_table=deployed_tile_table,
                start_ts=start_ts_1,
                end_ts=end_ts_1,
                update_last_run_metadata=False,
                update_backfill_start_date=True,
            )

        if start_ts_2 is not None and end_ts_2 is not None:
            await self._backfill_tiles_with_start_end(
                session=session,
                deployed_tile_table=deployed_tile_table,
                start_ts=start_ts_2,
                end_ts=end_ts_2,
                update_last_run_metadata=True,
                update_backfill_start_date=to_init_backfill_metadata,
            )

    async def _backfill_tiles_with_start_end(
        self,
        session: BaseSession,
        deployed_tile_table: DeployedTileTableModel,
        start_ts: datetime,
        end_ts: datetime,
        update_last_run_metadata: bool,
        update_backfill_start_date: bool,
    ) -> None:
        # Align the start timestamp to the tile boundary
        tile_spec = deployed_tile_table.to_tile_spec()
        start_ind = date_util.timestamp_utc_to_tile_index(
            start_ts,
            tile_spec.time_modulo_frequency_second,
            tile_spec.blind_spot_second,
            tile_spec.frequency_minute,
        )
        start_ts = date_util.tile_index_to_timestamp_utc(
            start_ind,
            tile_spec.time_modulo_frequency_second,
            tile_spec.blind_spot_second,
            tile_spec.frequency_minute,
        )
        start_ts_str = start_ts.isoformat()
        end_ts_str = end_ts.isoformat()
        await self.tile_manager_service.generate_tiles(
            session=session,
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            end_ts_str=end_ts_str,
            start_ts_str=start_ts_str,
            deployed_tile_table_id=deployed_tile_table.id,
            update_last_run_metadata=update_last_run_metadata,
        )
        if update_backfill_start_date:
            await self.deployed_tile_table_service.update_backfill_metadata(
                document_id=deployed_tile_table.id,
                backfill_start_date=start_ts,
            )
        await self.feature_service.update_last_updated_by_scheduled_task_at(
            aggregation_ids=deployed_tile_table.aggregation_ids,
            last_updated_by_scheduled_task_at=datetime.utcnow(),
        )

    async def online_disable(self, session: Optional[BaseSession], feature: FeatureModel) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        session: Optional[BaseSession]
            Instance of BaseSession to interact with the data warehouse
        feature: FeatureModel
            Instance of FeatureModel
        """
        if (
            FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED
            and session
            and session.source_type == SourceType.DATABRICKS_UNITY
        ):
            # Remove Databricks UDF for on-demand feature
            assert isinstance(session, DatabricksUnitySession)
            await self.remove_databricks_udf_for_on_demand_feature_if_exists(session, feature)

        # cleaning online store compute queries
        await self.remove_online_store_compute_queries(feature.aggregation_result_names)

        # disable tile scheduled jobs
        for aggregation_id in feature.aggregation_ids:
            # disable legacy tile jobs if any
            await self.tile_manager_service.remove_legacy_tile_jobs(aggregation_id)

        await self.remove_online_store_cleanup_jobs(session, feature.online_store_table_names)

    async def remove_online_store_compute_queries(
        self, aggregation_result_names: List[str]
    ) -> None:
        """
        Update the list of currently active online store compute queries

        Parameters
        ----------
        aggregation_result_names: List[str]
            Aggregation result names that identify the online store compute queries to be removed
        """
        if not aggregation_result_names:
            return
        query_filter = {
            "online_enabled": True,
            "aggregation_result_names": {
                "$in": list(aggregation_result_names),
            },
        }
        aggregation_result_names_still_in_use = set()
        async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter=query_filter,
            projection={"aggregation_result_names": 1},
        ):
            aggregation_result_names_still_in_use.update(feature_doc["aggregation_result_names"])

        for result_name in aggregation_result_names:
            if result_name not in aggregation_result_names_still_in_use:
                try:
                    await self.online_store_compute_query_service.delete_by_result_name(result_name)
                except DocumentNotFoundError:
                    # Backward compatibility for features created before the queries are managed by
                    # OnlineStoreComputeQueryService
                    pass

    async def remove_online_store_cleanup_jobs(
        self, session: Optional[BaseSession], online_store_table_names: List[str]
    ) -> None:
        """
        Stop online store cleanup jobs if no longer referenced by other online enabled features

        Parameters
        ----------
        session: Optional[BaseSession]
            Instance of BaseSession to interact with the data warehouse
        online_store_table_names: List[str]
            List of online store tables to be cleaned up if no longer in use
        """
        if not online_store_table_names:
            return
        query_filter = {
            "online_enabled": True,
            "online_store_table_names": {
                "$in": list(online_store_table_names),
            },
        }
        online_store_table_names_still_in_use = set()
        async for feature_doc in self.feature_service.list_documents_as_dict_iterator(
            query_filter=query_filter,
            projection={"online_store_table_names": 1},
        ):
            online_store_table_names_still_in_use.update(feature_doc["online_store_table_names"])

        for table_name in online_store_table_names:
            if table_name not in online_store_table_names_still_in_use:
                await self.online_store_cleanup_scheduler_service.stop_job(table_name)
                if session is not None:
                    await session.execute_query_long_running(f"DROP TABLE IF EXISTS {table_name}")

    @staticmethod
    async def may_register_databricks_udf_for_on_demand_feature(
        session: DatabricksUnitySession, feature: FeatureModel
    ) -> None:
        """
        Register Databricks UDF for on-demand feature.

        Parameters
        ----------
        session: DatabricksUnitySession
            Instance of DatabricksUnitySession to interact with the data warehouse
        feature: FeatureModel
            Instance of FeatureModel
        """
        offline_store_info = feature.offline_store_info
        if offline_store_info and offline_store_info.udf_info and offline_store_info.udf_info.codes:
            udf_info = offline_store_info.udf_info
            logger.debug(
                "Registering Databricks UDF for on-demand feature",
                extra={
                    "feature_name": feature.name,
                    "function_name": udf_info.sql_function_name,
                },
            )
            await session.execute_query(f"DROP FUNCTION IF EXISTS {udf_info.sql_function_name}")
            await session.execute_query(udf_info.codes)

    @staticmethod
    async def remove_databricks_udf_for_on_demand_feature_if_exists(
        session: DatabricksUnitySession, feature: FeatureModel
    ) -> None:
        """
        Remove Databricks UDF for on-demand feature.

        Parameters
        ----------
        session: DatabricksUnitySession
            Instance of DatabricksUnitySession to interact with the data warehouse
        feature: FeatureModel
            Instance of FeatureModel
        """
        offline_store_info = feature.offline_store_info
        if (
            offline_store_info
            and offline_store_info.udf_info
            and offline_store_info.udf_info.sql_function_name
        ):
            udf_info = offline_store_info.udf_info
            logger.debug(
                "Removing Databricks UDF for on-demand feature",
                extra={
                    "feature_name": feature.name,
                    "function_name": udf_info.sql_function_name,
                },
            )
            await session.execute_query(f"DROP FUNCTION IF EXISTS {udf_info.sql_function_name}")
