"""
Feature Manager class
"""
from __future__ import annotations

from typing import Any

from datetime import datetime, timedelta, timezone

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.common import date_util
from featurebyte.common.date_util import get_next_job_datetime
from featurebyte.common.tile_util import tile_manager_from_session
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.sql_template import (
    tm_call_schedule_online_store,
    tm_delete_online_store_mapping,
    tm_delete_tile_feature_mapping,
    tm_feature_tile_monitor,
    tm_last_tile_index,
    tm_upsert_online_store_mapping,
    tm_upsert_tile_feature_mapping,
)
from featurebyte.logger import logger
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.base import BaseTileManager
from featurebyte.utils.snowflake.sql import escape_column_names


class FeatureManager(BaseModel):
    """
    Snowflake Feature Manager class
    """

    _session: BaseSession = PrivateAttr()
    _tile_manager: BaseTileManager = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session
        self._tile_manager = tile_manager_from_session(session)

    async def online_enable(
        self, feature_spec: OnlineFeatureSpec, schedule_time: datetime = datetime.utcnow()
    ) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        schedule_time: datetime
            the moment of scheduling the job
        """
        logger.info(f"online_enable: {feature_spec.feature.name}")

        user_id = feature_spec.feature.user_id
        feature_store_id = feature_spec.feature.tabular_source.feature_store_id
        workspace_id = feature_spec.feature.workspace_id

        # insert records into tile-feature mapping table
        await self._update_tile_feature_mapping_table(feature_spec)

        # enable tile generation with scheduled jobs
        for tile_spec in feature_spec.feature.tile_specs:

            exist_tasks = await self._session.execute_query(
                f"SHOW TASKS LIKE '%{tile_spec.aggregation_id}%'"
            )
            if exist_tasks is None or len(exist_tasks) == 0:
                # enable online tiles scheduled job
                await self._tile_manager.schedule_online_tiles(
                    tile_spec=tile_spec,
                    schedule_time=schedule_time,
                    user_id=user_id,
                    feature_store_id=feature_store_id,
                    workspace_id=workspace_id,
                )
                logger.debug(f"Done schedule_online_tiles for {tile_spec.aggregation_id}")

                # enable offline tiles scheduled job
                await self._tile_manager.schedule_offline_tiles(
                    tile_spec=tile_spec,
                    schedule_time=schedule_time,
                    user_id=user_id,
                    feature_store_id=feature_store_id,
                    workspace_id=workspace_id,
                )
                logger.debug(f"Done schedule_offline_tiles for {tile_spec.aggregation_id}")

            # generate historical tiles
            await self._generate_historical_tiles(tile_spec=tile_spec)

            # populate feature store
            await self._populate_feature_store(tile_spec=tile_spec, schedule_time=schedule_time)

    async def _populate_feature_store(self, tile_spec: TileSpec, schedule_time: datetime) -> None:
        next_job_time = get_next_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=tile_spec.frequency_minute,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        job_schedule_ts = next_job_time - timedelta(minutes=tile_spec.frequency_minute)
        job_schedule_ts_str = job_schedule_ts.strftime("%Y-%m-%d %H:%M:%S")

        populate_sql = tm_call_schedule_online_store.render(
            aggregation_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
        await self._session.execute_query(populate_sql)

    async def _generate_historical_tiles(self, tile_spec: TileSpec) -> None:
        """
        Generate historical tiles for a given tile_spec

        Parameters
        ----------
        tile_spec: TileSpec
            input tile spec
        """
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

        await self._tile_manager.generate_tiles(
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            end_ts_str=end_ts_str,
            start_ts_str=start_ts_str,
        )

    async def _update_tile_feature_mapping_table(self, feature_spec: OnlineFeatureSpec) -> None:
        """
        Insert records into tile-feature mapping table

        Parameters
        ----------
        feature_spec: OnlineFeatureSpec
            Instance of OnlineFeatureSpec
        """
        for tile_spec in feature_spec.feature.tile_specs:
            upsert_sql = tm_upsert_tile_feature_mapping.render(
                tile_id=tile_spec.tile_id,
                aggregation_id=tile_spec.aggregation_id,
                feature_name=feature_spec.feature.name,
                feature_type=feature_spec.value_type,
                feature_version=feature_spec.feature.version.to_str(),
                feature_readiness=str(feature_spec.feature.readiness),
                feature_event_data_ids=",".join([str(i) for i in feature_spec.event_data_ids]),
                is_deleted=False,
            )
            await self._session.execute_query(upsert_sql)
            logger.debug(f"Done insert tile_feature_mapping for {tile_spec.aggregation_id}")

        for query in feature_spec.precompute_queries:
            upsert_sql = tm_upsert_online_store_mapping.render(
                tile_id=query.tile_id,
                aggregation_id=query.aggregation_id,
                result_id=query.result_name,
                result_type=query.result_type,
                sql_query=query.sql.replace("'", "''"),
                online_store_table_name=query.table_name,
                entity_column_names=",".join(escape_column_names(query.serving_names)),
                is_deleted=False,
            )
            await self._session.execute_query(upsert_sql)
            logger.debug(f"Done insert tile_feature_mapping for {query.result_name}")

    async def online_disable(self, feature_spec: OnlineFeatureSpec) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        feature_spec: OnlineFeatureSpec
            input feature instance
        """
        # delete records from tile-feature mapping table
        for agg_id in feature_spec.aggregation_ids:
            delete_sql = tm_delete_tile_feature_mapping.render(
                aggregation_id=agg_id,
                feature_name=feature_spec.feature.name,
                feature_version=feature_spec.feature.version.to_str(),
            )
            await self._session.execute_query(delete_sql)
            logger.debug(f"Done delete tile_feature_mapping for {agg_id}")
            delete_sql = tm_delete_online_store_mapping.render(aggregation_id=agg_id)
            await self._session.execute_query(delete_sql)
            logger.debug(f"Done delete online_store_mapping for {agg_id}")

        # disable tile scheduled jobs
        for tile_spec in feature_spec.feature.tile_specs:
            await self._tile_manager.remove_tile_jobs(tile_spec)

    async def retrieve_last_tile_index(self, feature: ExtendedFeatureModel) -> pd.DataFrame:
        """
        Get last_tile_index of all the tile_ids as dataframe

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance

        Returns
        -------
            last_tile_index of all the tile_ids as dataframe
        """
        sql = tm_last_tile_index.render(feature=feature)
        result = await self._session.execute_query(sql)
        return result

    async def retrieve_feature_tile_inconsistency_data(
        self, query_start_ts: str, query_end_ts: str
    ) -> pd.DataFrame:
        """
        Retrieve the raw data of feature tile inconsistency monitoring

        Parameters
        ----------
        query_start_ts: str
            start monitoring timestamp of tile inconsistency
        query_end_ts: str
            end monitoring timestamp of tile inconsistency

        Returns
        -------
            raw data of feature-tile inconsistency as dataframe
        """
        sql = tm_feature_tile_monitor.render(
            query_start_ts=query_start_ts, query_end_ts=query_end_ts
        )
        result = await self._session.execute_query(sql)
        return result
