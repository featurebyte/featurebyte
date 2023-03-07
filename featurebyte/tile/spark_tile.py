"""
Spark Tile class
"""
from typing import Any, Optional

from pydantic import PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.session.spark import SparkSession
from featurebyte.sql.spark.tile_generate import TileGenerate
from featurebyte.sql.spark.tile_generate_entity_tracking import TileGenerateEntityTracking
from featurebyte.sql.spark.tile_schedule_online_store import TileScheduleOnlineStore
from featurebyte.tile.base import BaseTileManager


class TileManagerSpark(BaseTileManager):
    """
    Spark Tile class
    """

    _session: SparkSession = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileManagerSpark to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(session=session, **kw)

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
        # TODO: implement this
        return True

    async def populate_feature_store(self, tile_spec: TileSpec, job_schedule_ts_str: str) -> None:
        """
        Populate feature store with the given tile_spec and timestamp string

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        job_schedule_ts_str: str
            timestamp string of the job schedule
        """
        executor = TileScheduleOnlineStore(
            spark_session=self._session,
            agg_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
        await executor.execute()

    async def update_tile_entity_tracker(self, tile_spec: TileSpec, temp_entity_table: str) -> str:
        """
        Update <tile_id>_entity_tracker table for last_tile_start_date

        Parameters
        ----------
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
            spark_session=self._session,
            tile_id=tile_spec.aggregation_id,
            entity_column_names=entity_column_names,
            entity_table=temp_entity_table,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
        )

        await tile_entity_tracking_ins.execute()

        return tile_entity_tracking_ins.json()

    async def generate_tiles(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        last_tile_start_ts_str: Optional[str] = None,
    ) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
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

        Returns
        -------
            job run details
        """

        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        if last_tile_start_ts_str:
            last_tile_start_ts_str = f"'{last_tile_start_ts_str}'"

        tile_generate_ins = TileGenerate(
            spark_session=self._session,
            tile_id=tile_spec.tile_id,
            tile_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            sql=tile_sql,
            entity_column_names=tile_spec.entity_column_names,
            value_column_names=tile_spec.value_column_names,
            value_column_types=tile_spec.value_column_types,
            tile_type=tile_type,
            tile_start_date_column=InternalName.TILE_START_DATE,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
            last_tile_start_ts_str=last_tile_start_ts_str,
        )
        await tile_generate_ins.execute()

        return tile_generate_ins.json()
