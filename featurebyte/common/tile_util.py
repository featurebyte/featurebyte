"""
Tile related common utility function
"""
from __future__ import annotations

from typing import Optional

from featurebyte import SourceType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.tile.base import BaseTileManager
from featurebyte.tile.snowflake_tile import TileManagerSnowflake
from featurebyte.tile.spark_tile import TileManagerSpark


def tile_manager_from_session(
    session: BaseSession,
    task_manager: Optional[TaskManager] = None,
    use_snowflake_scheduling: Optional[bool] = True,
) -> BaseTileManager:
    """
    Derive implementing TileManager instance based on input sessions

    Parameters
    ----------
    session: BaseSession
        Input session
    task_manager: Optional[TaskManager]
        Input task manager
    use_snowflake_scheduling: Optional[bool]
        use snowflake scheduling

    Returns
    -------
    corresponding TileManager instance

    Raises
    -------
    ValueError
        if TileManager for session source type is not implemented yet
    """

    if session.source_type == SourceType.SNOWFLAKE:
        flag = True if not use_snowflake_scheduling else use_snowflake_scheduling
        return TileManagerSnowflake(
            session=session, task_manager=task_manager, use_snowflake_scheduling=flag
        )

    if session.source_type == SourceType.SPARK:
        return TileManagerSpark(session=session, task_manager=task_manager)

    raise ValueError(f"Tile Manager for {session.source_type} has not been implemented")
