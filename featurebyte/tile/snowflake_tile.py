"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import PrivateAttr

from featurebyte.feature_manager.sql_template import tm_call_schedule_online_store
from featurebyte.models.tile import TileSpec
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.base import BaseTileManager


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
