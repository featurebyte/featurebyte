"""
FeatureManagerService class
"""
from __future__ import annotations

from typing import Any, List, Optional, Set

from datetime import datetime, timedelta, timezone

import pandas as pd
from bson import ObjectId
from sqlglot import expressions
from sqlglot.expressions import select

from featurebyte.common import date_util
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.feature_manager.sql_template import (
    tm_delete_online_store_mapping,
    tm_feature_tile_monitor,
    tm_upsert_online_store_mapping,
)
from featurebyte.logging import get_logger
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.persistent import Persistent
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.online_serving import OnlineStorePrecomputeQuery
from featurebyte.service.base_service import BaseService
from featurebyte.service.tile_manager import TileManagerService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.session.base import BaseSession
from featurebyte.utils.snowflake.sql import escape_column_names

logger = get_logger(__name__)


class FeatureManagerService(BaseService):
    """
    FeatureManagerService is responsible for orchestrating the materialization of features and tiles
    when a feature is online enabled or disabled.
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        tile_manager_service: TileManagerService,
        tile_registry_service: TileRegistryService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.tile_manager_service = tile_manager_service
        self.tile_registry_service = tile_registry_service

    async def online_enable(
        self,
        session: BaseSession,
        feature_spec: OnlineFeatureSpec,
        schedule_time: Optional[datetime] = None,
        is_recreating_schema: bool = False,
    ) -> None:
        """
        Schedule both online and offline tile jobs

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
        if schedule_time is None:
            schedule_time = datetime.utcnow()

        logger.info(
            "Online enabling a feature",
            extra={"feature_name": feature_spec.feature.name, "schedule_time": schedule_time},
        )

        # get aggregation result names that need to be populated
        unscheduled_result_names = await self._get_unscheduled_aggregation_result_names(
            session, feature_spec
        )
        logger.debug(
            "Done retrieving unscheduled aggregation result names",
            extra={"unscheduled_result_names": list(unscheduled_result_names)},
        )

        # insert records into tile-feature mapping table
        await self._update_tile_feature_mapping_table(
            session, feature_spec, unscheduled_result_names
        )

        # enable tile generation with scheduled jobs
        aggregation_id_to_tile_spec = {}
        for tile_spec in feature_spec.feature.tile_specs:
            aggregation_id_to_tile_spec[tile_spec.aggregation_id] = tile_spec
            tile_job_exists = await self.tile_manager_service.tile_job_exists(tile_spec=tile_spec)
            if not tile_job_exists:
                # enable online tiles scheduled job
                await self.tile_manager_service.schedule_online_tiles(tile_spec=tile_spec)
                logger.debug(f"Done schedule_online_tiles for {tile_spec.aggregation_id}")

                # enable offline tiles scheduled job
                await self.tile_manager_service.schedule_offline_tiles(tile_spec=tile_spec)
                logger.debug(f"Done schedule_offline_tiles for {tile_spec.aggregation_id}")

                # generate historical tiles
                await self._generate_historical_tiles(session=session, tile_spec=tile_spec)

            elif is_recreating_schema:
                # if this is called when recreating the schema, we cannot assume that the historical
                # tiles are available even if there is an active tile jobs.
                await self._generate_historical_tiles(session=session, tile_spec=tile_spec)

        # populate feature store
        for query in self._get_unscheduled_precompute_queries(
            feature_spec, unscheduled_result_names
        ):
            await self._populate_feature_store(
                session=session,
                tile_spec=aggregation_id_to_tile_spec[query.aggregation_id],
                schedule_time=schedule_time,
                aggregation_result_name=query.result_name,
            )

    @staticmethod
    async def _get_unscheduled_aggregation_result_names(
        session: BaseSession, feature_spec: OnlineFeatureSpec
    ) -> Set[str]:
        """
        Get the aggregation result names that are not yet scheduled

        This means that we need to run a one-off job to populate the online store for them.
        Otherwise, there is nothing do to as one of the scheduled tile jobs would have already
        computed them.

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec

        Returns
        -------
        Set[str]
        """
        result_names = [query.result_name for query in feature_spec.precompute_queries]
        query = (
            select("RESULT_ID")
            .from_("ONLINE_STORE_MAPPING")
            .where(
                expressions.In(
                    this=expressions.Identifier(this="RESULT_ID"),
                    expressions=[make_literal_value(result_name) for result_name in result_names],
                ),
            )
        )
        df_scheduled_result_names = await session.execute_query(
            sql_to_string(query, session.source_type)
        )
        if df_scheduled_result_names is None:
            return set(result_names)
        return set(result_names) - set(df_scheduled_result_names["RESULT_ID"])

    @staticmethod
    def _get_unscheduled_precompute_queries(
        feature_spec: OnlineFeatureSpec, unscheduled_result_names: Set[str]
    ) -> List[OnlineStorePrecomputeQuery]:
        out = []
        for query in feature_spec.precompute_queries:
            if query.result_name in unscheduled_result_names:
                out.append(query)
        return out

    async def _populate_feature_store(
        self,
        session: BaseSession,
        tile_spec: TileSpec,
        schedule_time: datetime,
        aggregation_result_name: str,
    ) -> None:
        next_job_time = get_next_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=tile_spec.frequency_minute,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        job_schedule_ts = next_job_time - timedelta(minutes=tile_spec.frequency_minute)
        job_schedule_ts_str = job_schedule_ts.strftime("%Y-%m-%d %H:%M:%S")
        await self.tile_manager_service.populate_feature_store(
            session, tile_spec, job_schedule_ts_str, aggregation_result_name
        )

    async def _generate_historical_tiles(self, session: BaseSession, tile_spec: TileSpec) -> None:
        # generate historical tile_values
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        # derive the latest tile_start_date
        end_ind = date_util.timestamp_utc_to_tile_index(
            datetime.utcnow(),
            tile_spec.time_modulo_frequency_second,
            tile_spec.blind_spot_second,
            tile_spec.frequency_minute,
        )
        end_ts = date_util.tile_index_to_timestamp_utc(
            end_ind,
            tile_spec.time_modulo_frequency_second,
            tile_spec.blind_spot_second,
            tile_spec.frequency_minute,
        )
        end_ts_str = end_ts.strftime(date_format)

        start_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

        tile_model = await self.tile_registry_service.get_tile_model(
            tile_spec.tile_id, tile_spec.aggregation_id
        )
        if tile_model is not None and tile_model.last_tile_metadata_offline is not None:
            start_ts = tile_model.last_tile_metadata_offline.start_date

        logger.info(f"start_ts: {start_ts}")

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
        start_ts_str = start_ts.strftime(date_format)

        await self.tile_manager_service.generate_tiles(
            session=session,
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            end_ts_str=end_ts_str,
            start_ts_str=start_ts_str,
            last_tile_start_ts_str=end_ts_str,
        )

    async def _update_tile_feature_mapping_table(
        self,
        session: BaseSession,
        feature_spec: OnlineFeatureSpec,
        unscheduled_result_names: Set[str],
    ) -> None:
        """
        Insert records into tile-feature mapping table

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        unscheduled_result_names: Set[str]
            Set of unscheduled result names. These result names are not in the online store mapping
            table yet and should be inserted.
        """
        for query in self._get_unscheduled_precompute_queries(
            feature_spec, unscheduled_result_names
        ):
            upsert_sql = tm_upsert_online_store_mapping.render(
                tile_id=query.tile_id,
                aggregation_id=query.aggregation_id,
                result_id=query.result_name,
                result_type=query.result_type,
                sql_query=get_sql_adapter(session.source_type).escape_quote_char(query.sql),
                online_store_table_name=query.table_name,
                entity_column_names=",".join(escape_column_names(query.serving_names)),
            )
            await session.execute_query(upsert_sql)
            logger.debug(f"Done insert tile_feature_mapping for {query.result_name}")

    async def online_disable(self, session: BaseSession, feature_spec: OnlineFeatureSpec) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        session: BaseSession
            Instance of BaseSession to interact with the data warehouse
        feature_spec: OnlineFeatureSpec
            input feature instance
        """
        # delete records from tile-feature mapping table
        for agg_id in feature_spec.aggregation_ids:
            for query in feature_spec.precompute_queries:
                delete_sql = tm_delete_online_store_mapping.render(result_id=query.result_name)
                await session.execute_query(delete_sql)
            logger.debug(f"Done delete online_store_mapping for {agg_id}")

        # disable tile scheduled jobs
        for tile_spec in feature_spec.feature.tile_specs:
            await self.tile_manager_service.remove_tile_jobs(tile_spec)

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
