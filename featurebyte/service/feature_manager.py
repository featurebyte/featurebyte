"""
FeatureManagerService class
"""

from __future__ import annotations

from typing import List, Optional, Set

from datetime import datetime, timedelta, timezone

import pandas as pd
from bson import ObjectId

from featurebyte.common import date_util
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import SourceType
from featurebyte.exception import DocumentNotFoundError
from featurebyte.feature_manager.sql_template import tm_feature_tile_monitor
from featurebyte.logging import get_logger
from featurebyte.models.deployment import FeastIntegrationSettings
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.models.online_store_spec import OnlineFeatureSpec
from featurebyte.models.tile import TileSpec, TileType
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
    ):
        self.catalog_id = catalog_id
        self.tile_manager_service = tile_manager_service
        self.tile_registry_service = tile_registry_service
        self.online_store_compute_query_service = online_store_compute_query_service
        self.online_store_cleanup_scheduler_service = online_store_cleanup_scheduler_service
        self.feature_service = feature_service

    async def online_enable(
        self,
        session: BaseSession,
        feature_spec: OnlineFeatureSpec,
        schedule_time: Optional[datetime] = None,
        is_recreating_schema: bool = False,
    ) -> None:
        """
        Task to update data warehouse when the feature is online enabled (e.g. schedule tile jobs)

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        schedule_time: Optional[datetime]
            the moment of scheduling the job
        is_recreating_schema: bool
            Whether we are recreating the working schema from scratch. Only set as True when called
            by WorkingSchemaService.
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
            extra={"feature_name": feature_spec.feature.name, "schedule_time": schedule_time},
        )

        # get aggregation result names that need to be populated
        unscheduled_result_names = await self._get_unscheduled_aggregation_result_names(
            feature_spec
        )
        logger.debug(
            "Done retrieving unscheduled aggregation result names",
            extra={"unscheduled_result_names": list(unscheduled_result_names)},
        )

        # insert records into tile-feature mapping table
        await self._update_tile_feature_mapping_table(feature_spec, unscheduled_result_names)

        # backfill historical tiles if required
        aggregation_id_to_tile_spec = {}
        tile_specs_to_be_scheduled = []
        for tile_spec in feature_spec.feature.tile_specs:
            aggregation_id_to_tile_spec[tile_spec.aggregation_id] = tile_spec
            tile_job_exists = await self.tile_manager_service.tile_job_exists(tile_spec=tile_spec)
            if not tile_job_exists:
                tile_specs_to_be_scheduled.append(tile_spec)
            await self._backfill_tiles(
                session=session, tile_spec=tile_spec, schedule_time=schedule_time
            )

        # populate feature store. if this is called when recreating the schema, we need to run all
        # the online store compute queries since the tables need to be regenerated.
        for query in self._filter_precompute_queries(
            feature_spec, None if is_recreating_schema else unscheduled_result_names
        ):
            await self._populate_feature_store(
                session=session,
                tile_spec=aggregation_id_to_tile_spec[query.aggregation_id],
                schedule_time=schedule_time,
                aggregation_result_name=query.result_name,
            )
            if is_recreating_schema:
                # If re-creating schema, the cleanup job would have been already scheduled, and this
                # part can be skipped
                continue
            assert query.feature_store_id is not None
            await self.online_store_cleanup_scheduler_service.start_job_if_not_exist(
                catalog_id=self.catalog_id,
                feature_store_id=query.feature_store_id,
                online_store_table_name=query.table_name,
            )

        # enable tile generation with scheduled jobs
        for tile_spec in tile_specs_to_be_scheduled:
            # enable online tiles scheduled job
            await self.tile_manager_service.schedule_online_tiles(tile_spec=tile_spec)
            logger.debug(f"Done schedule_online_tiles for {tile_spec.aggregation_id}")

            # enable offline tiles scheduled job
            await self.tile_manager_service.schedule_offline_tiles(tile_spec=tile_spec)
            logger.debug(f"Done schedule_offline_tiles for {tile_spec.aggregation_id}")

    async def _get_unscheduled_aggregation_result_names(
        self, feature_spec: OnlineFeatureSpec
    ) -> Set[str]:
        """
        Get the aggregation result names that are not yet scheduled

        This means that we need to run a one-off job to populate the online store for them.
        Otherwise, there is nothing do to as one of the scheduled tile jobs would have already
        computed them.

        Parameters
        ----------
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec

        Returns
        -------
        Set[str]
        """
        result_names = {query.result_name for query in feature_spec.precompute_queries}
        async for query in self.online_store_compute_query_service.list_by_result_names(
            list(result_names)
        ):
            result_names.remove(query.result_name)
        return result_names

    @staticmethod
    def _filter_precompute_queries(
        feature_spec: OnlineFeatureSpec, includes: Optional[Set[str]]
    ) -> List[OnlineStoreComputeQueryModel]:
        out = []
        for query in feature_spec.precompute_queries:
            if includes is None or query.result_name in includes:
                out.append(query)
        return out

    async def _populate_feature_store(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        schedule_time: datetime,
        aggregation_result_name: str,
    ) -> None:
        job_schedule_ts = get_previous_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=tile_spec.frequency_minute,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        job_schedule_ts_str = job_schedule_ts.strftime("%Y-%m-%d %H:%M:%S")
        await self.tile_manager_service.populate_feature_store(
            session, tile_spec, job_schedule_ts_str, aggregation_result_name
        )

    async def _backfill_tiles(  # pylint: disable=too-many-branches
        self, session: BaseSession, tile_spec: TileSpec, schedule_time: datetime
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
        tile_spec: TileSpec
            Instance of TileSpec
        schedule_time: datetime
            The moment of enabling the feature
        """
        # Derive the tile end date based on expected previous job time
        job_schedule_ts = get_previous_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=tile_spec.frequency_minute,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        end_ts = job_schedule_ts - timedelta(seconds=tile_spec.blind_spot_second)

        # Determine the earliest tiles required to compute the features for offline store. This is
        # determined by the largest feature derivation window.
        max_window_seconds: Optional[int] = None
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

        tile_model = await self.tile_registry_service.get_tile_model(
            tile_spec.tile_id, tile_spec.aggregation_id
        )
        if tile_model is not None and tile_model.backfill_metadata is not None:
            if start_ts.replace(tzinfo=None) < tile_model.backfill_metadata.start_date:
                # Need to compute tiles up to previous backfill start date
                start_ts_1 = start_ts
                end_ts_1 = tile_model.backfill_metadata.start_date
            if tile_model.last_run_metadata_offline is None:
                # Note: unlikely to happen as there should already be a tile job running, but we
                # still need to handle this case in case of bad state / error
                start_ts_2 = tile_model.backfill_metadata.start_date
                end_ts_2 = end_ts
            elif tile_model.last_run_metadata_offline.tile_end_date < end_ts.replace(tzinfo=None):
                # Need to compute tiles from last run tile end date to end_ts
                start_ts_2 = tile_model.last_run_metadata_offline.tile_end_date
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
                tile_spec=tile_spec,
                start_ts=start_ts_1,
                end_ts=end_ts_1,
                update_last_run_metadata=False,
                update_backfill_start_date=True,
            )

        if start_ts_2 is not None and end_ts_2 is not None:
            await self._backfill_tiles_with_start_end(
                session=session,
                tile_spec=tile_spec,
                start_ts=start_ts_2,
                end_ts=end_ts_2,
                update_last_run_metadata=True,
                update_backfill_start_date=to_init_backfill_metadata,
            )

    async def _backfill_tiles_with_start_end(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        start_ts: datetime,
        end_ts: datetime,
        update_last_run_metadata: bool,
        update_backfill_start_date: bool,
    ) -> None:
        # Align the start timestamp to the tile boundary
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
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        start_ts_str = start_ts.strftime(date_format)
        end_ts_str = end_ts.strftime(date_format)
        await self.tile_manager_service.generate_tiles(
            session=session,
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            end_ts_str=end_ts_str,
            start_ts_str=start_ts_str,
            last_tile_start_ts_str=end_ts_str if update_last_run_metadata else None,
        )
        if update_backfill_start_date:
            await self.tile_registry_service.update_backfill_metadata(
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                backfill_start_date=start_ts,
            )

    async def _update_tile_feature_mapping_table(
        self,
        feature_spec: OnlineFeatureSpec,
        unscheduled_result_names: Set[str],
    ) -> None:
        """
        Insert records into tile-feature mapping table

        Parameters
        ----------
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        unscheduled_result_names: Set[str]
            Set of unscheduled result names. These result names are not in the online store mapping
            table yet and should be inserted.
        """
        for query in self._filter_precompute_queries(feature_spec, unscheduled_result_names):
            query.feature_store_id = feature_spec.feature.tabular_source.feature_store_id
            await self.online_store_compute_query_service.create_document(query)

    async def online_disable(
        self, session: Optional[BaseSession], feature_spec: OnlineFeatureSpec
    ) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        session: Optional[BaseSession]
            Instance of BaseSession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            input feature instance
        """
        if (
            FeastIntegrationSettings().FEATUREBYTE_FEAST_INTEGRATION_ENABLED
            and session
            and session.source_type == SourceType.DATABRICKS_UNITY
        ):
            # Remove Databricks UDF for on-demand feature
            assert isinstance(session, DatabricksUnitySession)
            await self.remove_databricks_udf_for_on_demand_feature_if_exists(session, feature_spec)

        # cleaning online store compute queries
        await self.remove_online_store_compute_queries(
            feature_spec.feature.aggregation_result_names
        )

        # disable tile scheduled jobs
        for aggregation_id in feature_spec.feature.aggregation_ids:
            await self.tile_manager_service.remove_tile_jobs(aggregation_id)

        await self.remove_online_store_cleanup_jobs(
            session, feature_spec.feature.online_store_table_names
        )

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
                    await session.execute_query(f"DROP TABLE IF EXISTS {table_name}")

    @staticmethod
    async def retrieve_feature_tile_inconsistency_data(
        session: BaseSession, query_start_ts: str, query_end_ts: str
    ) -> pd.DataFrame:
        """
        Retrieve the raw table of feature tile inconsistency monitoring

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        query_start_ts: str
            start monitoring timestamp of tile inconsistency
        query_end_ts: str
            end monitoring timestamp of tile inconsistency

        Returns
        -------
            raw table of feature-tile inconsistency as dataframe
        """
        sql = tm_feature_tile_monitor.render(
            query_start_ts=query_start_ts, query_end_ts=query_end_ts
        )
        result = await session.execute_query(sql)
        return result

    @staticmethod
    async def may_register_databricks_udf_for_on_demand_feature(
        session: DatabricksUnitySession, feature_spec: OnlineFeatureSpec
    ) -> None:
        """
        Register Databricks UDF for on-demand feature.

        Parameters
        ----------
        session: DatabricksUnitySession
            Instance of DatabricksUnitySession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        """
        offline_store_info = feature_spec.feature.offline_store_info
        if offline_store_info and offline_store_info.udf_info and offline_store_info.udf_info.codes:
            udf_info = offline_store_info.udf_info
            logger.debug(
                "Registering Databricks UDF for on-demand feature",
                extra={
                    "feature_name": feature_spec.feature.name,
                    "function_name": udf_info.sql_function_name,
                },
            )
            await session.execute_query(f"DROP FUNCTION IF EXISTS {udf_info.sql_function_name}")
            await session.execute_query(udf_info.codes)
            await session.set_owner("FUNCTION", udf_info.sql_function_name)

    @staticmethod
    async def remove_databricks_udf_for_on_demand_feature_if_exists(
        session: DatabricksUnitySession, feature_spec: OnlineFeatureSpec
    ) -> None:
        """
        Remove Databricks UDF for on-demand feature.

        Parameters
        ----------
        session: DatabricksUnitySession
            Instance of DatabricksUnitySession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        """
        offline_store_info = feature_spec.feature.offline_store_info
        if (
            offline_store_info
            and offline_store_info.udf_info
            and offline_store_info.udf_info.sql_function_name
        ):
            udf_info = offline_store_info.udf_info
            logger.debug(
                "Removing Databricks UDF for on-demand feature",
                extra={
                    "feature_name": feature_spec.feature.name,
                    "function_name": udf_info.sql_function_name,
                },
            )
            await session.execute_query(f"DROP FUNCTION IF EXISTS {udf_info.sql_function_name}")
