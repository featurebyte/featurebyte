"""
Snowflake Tile Manager class
"""
from __future__ import annotations

from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.snowflake_tile import TileSnowflake


class SnowflakeFeatureManager:
    """
    Snowflake Tile Manager class
    """

    def __init__(self, session: SnowflakeSession, tile: TileSnowflake) -> None:
        self._session = session
        self._tile = tile

    def online_enable(self, tile_id: str) -> None:
        """
        Schedule both online and offline tile jobs

        if not tile_id already exists
            insert new record
        else
            set enabled = 'Y'

        start both online and offline tile task

        Returns
        -------

        """
        r = self._session.execute_query(f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = {tile_id}")
        if len(r) == 0:
            pass
        else:
            self._session.execute_query(
                f"UPDATE TILE_REGISTRY SET ENABLED = 'Y' WHERE TILE_ID = {tile_id}"
            )

        self._tile.schedule_online_tiles()
        self._tile.schedule_offline_tiles()

    def online_disable(self, tile_id: str) -> None:
        """
        Disable both online and offline tile jobs. Update enabled status to disabled

        Returns
        -------

        """
        pass

    def get_last_tile_index(self, tile_id: str) -> int:
        """
        Get last_tile_index status of a tile_id

        Parameters
        ----------
        tile_id: str
            tile id

        Returns
        -------
            last tile index of the given tile_id
        """
        r = self._session.execute_query(
            f"SELECT LAST_TILE_INDEX FROM TILE_REGISTRY WHERE TILE_ID = {tile_id}"
        )
        if len(r) > 0:
            return r["LAST_TILE_INDEX"].iloc[0]
