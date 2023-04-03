"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.feature_manager.sql_template import tm_call_schedule_online_store
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.base import BaseTileManager
from featurebyte.tile.sql_template import tm_generate_tile
from featurebyte.utils.snowflake.sql import escape_column_names


class TileManagerSnowflake(BaseTileManager):
    """
    Snowflake Tile class
    """

    _session: SnowflakeSession = PrivateAttr()

    def __init__(
        self,
        session: BaseSession,
        task_manager: Optional[TaskManager] = None,
        **kw: Any,
    ) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        task_manager: Optional[TaskManager]
            input task manager
        kw: Any
            constructor arguments
        """
        super().__init__(session=session, task_manager=task_manager, **kw)

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
        populate_sql = tm_call_schedule_online_store.render(
            aggregation_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
        await self._session.execute_query(populate_sql)

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
            tile generation sql
        """
        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        tile_sql = tile_sql.replace("'", "''")

        if last_tile_start_ts_str:
            last_tile_start_ts_str = f"'{last_tile_start_ts_str}'"
        else:
            last_tile_start_ts_str = "null"

        logger.debug(f"last_tile_start_ts_str: {last_tile_start_ts_str}")

        sql = tm_generate_tile.render(
            tile_sql=tile_sql,
            tile_start_date_column=InternalName.TILE_START_DATE.value,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
            time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            entity_column_names=",".join(escape_column_names(tile_spec.entity_column_names)),
            value_column_names=",".join(tile_spec.value_column_names),
            value_column_types=",".join(tile_spec.value_column_types),
            tile_id=tile_spec.tile_id,
            tile_type=tile_type,
            last_tile_start_ts_str=last_tile_start_ts_str,
            aggregation_id=tile_spec.aggregation_id,
        )
        await self._session.execute_query(sql)

        return sql
