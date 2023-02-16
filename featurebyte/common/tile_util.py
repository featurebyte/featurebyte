"""
Tile related common utility function
"""
from __future__ import annotations

from featurebyte import SourceType
from featurebyte.session.base import BaseSession
from featurebyte.tile.base import BaseTileManager
from featurebyte.tile.snowflake_tile import TileManagerSnowflake
from featurebyte.tile.spark_tile import TileManagerSpark


def tile_manager_from_session(session: BaseSession) -> BaseTileManager:
    """
    Derive implementing TileManager instance based on input sessions

    Parameters
    ----------
    session: BaseSession
        Input session

    Returns
    -------
    corresponding TileManager instance

    Raises
    -------
    ValueError
        if TileManager for session source type is not implemented yet
    """

    if session.source_type == SourceType.SNOWFLAKE:
        return TileManagerSnowflake(session)

    if session.source_type == SourceType.SPARK:
        return TileManagerSpark(session)

    raise ValueError(f"Tile Manager for {session.source_type} has not been implemented")
