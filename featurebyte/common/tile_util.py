"""
Tile related common utility function
"""
from __future__ import annotations

from typing import Optional

from featurebyte import SourceType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.tile.manager import TileManager


def tile_manager_from_session(
    session: BaseSession,
    task_manager: Optional[TaskManager] = None,
) -> TileManager:
    """
    Derive implementing TileManager instance based on input sessions

    Parameters
    ----------
    session: BaseSession
        Input session
    task_manager: Optional[TaskManager]
        Input task manager

    Returns
    -------
    corresponding TileManager instance

    Raises
    -------
    ValueError
        if TileManager for session source type is not implemented yet
    """
    if session.source_type in [SourceType.SPARK, SourceType.SNOWFLAKE, SourceType.DATABRICKS]:
        return TileManager(session=session, task_manager=task_manager)

    raise ValueError(f"Tile Manager for {session.source_type} has not been implemented")
