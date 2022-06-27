"""
Snowflake Tile Manager class
"""
from __future__ import annotations

from featurebyte.models.event_data import Feature
from featurebyte.tile.snowflake_tile import TileSnowflake


class SnowflakeFeatureManager:
    """
    Snowflake Tile Manager class
    """

    def retrieve_feature(self, feature: Feature) -> None:
        pass

    def save_feature(self, feature: Feature) -> None:
        pass

    def online_enable(self, feature: Feature) -> None:
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
        tile_id = feature.tile_id
        r = self._session.execute_query(f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = {tile_id}")
        if len(r) == 0:
            pass
        else:
            self._session.execute_query(
                f"UPDATE TILE_REGISTRY SET ENABLED = 'Y' WHERE TILE_ID = {tile_id}"
            )

        session = feature.get_datasource().get_sessio

        tile_mgr = TileSnowflake(
            session=session,
            feature_name=feature.name,
            time_modulo_frequency_seconds=feature.time_modulo_frequency_second,
            blind_spot_seconds=feature.blind_spot_second,
            frequency_minute=feature.frequency_minute,
            tile_sql=feature.tile_sql,
            column_names=feature.column_names,
            tile_id=feature.tile_id,
        )

        tile_mgr.schedule_online_tiles()
        tile_mgr.schedule_offline_tiles()

    def online_disable(self, tile_id: str) -> None:
        """
        Disable both online and offline tile jobs. Update enabled status to disabled

        Returns
        -------

        """
        pass

    def get_last_tile_index(self, feature: Feature, tile_type: str) -> int:
        """
        Get last_tile_index status of a tile_id

        Parameters
        ----------
        feature: Feature
            instance of Feature object
        tile_type: str
            tile type. ie. ONLINE or OFFLINE

        Returns
        -------
            last tile index of the given tile_id
        """
        tile_id = feature.tile_id

        r = self._session.execute_query(
            f"SELECT LAST_TILE_INDEX_{tile_type} FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
        )
        if len(r) > 0:
            return r["LAST_TILE_INDEX"].iloc[0]
