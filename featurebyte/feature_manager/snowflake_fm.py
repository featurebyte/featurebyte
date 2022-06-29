"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.logger import logger
from featurebyte.models.event_data import Feature
from featurebyte.tile.snowflake_tile import TileSnowflake


class SnowflakeFeatureManager:
    """
    Snowflake Feature Manager class
    """

    def save_feature(self, feature: Feature, credentials: Credentials | None = None) -> None:
        """

        Parameters
        ----------
        feature
        credentials

        Returns
        -------

        """
        data_source = ExtendedDatabaseSourceModel(**feature.datasource.dict())
        session = data_source.get_session(credentials=credentials)
        _ = session

    def online_enable(self, feature: Feature, credentials: Credentials | None = None) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        feature: Feature
            input feature instance
        credentials: Credentials
            credentials of the database source

        Returns
        -------

        """
        tile_mgr = TileSnowflake(
            feature_name=feature.name,
            time_modulo_frequency_seconds=feature.time_modulo_frequency_second,
            blind_spot_seconds=feature.blind_spot_second,
            frequency_minute=feature.frequency_minute,
            tile_sql=feature.tile_sql,
            column_names=feature.column_names,
            tile_id=feature.tile_id,
            tabular_source=feature.datasource,
            credentials=credentials,
        )
        # insert tile_registry record
        tile_mgr.insert_tile_registry()
        logger.debug("Done insert_tile_registry")

        # enable online tiles scheduled job
        tile_mgr.schedule_online_tiles()
        logger.debug("Done schedule_online_tiles")

        # enable offline tiles scheduled job
        tile_mgr.schedule_offline_tiles()
        logger.debug("Done schedule_offline_tiles")

    def get_last_tile_index(
        self, feature: Feature, tile_type: str, credentials: Credentials | None = None
    ) -> int:
        """
        Get last_tile_index status of a tile_id

        Parameters
        ----------
        feature: Feature
            instance of Feature object
        tile_type: str
            tile type. ie. ONLINE or OFFLINE
        credentials: Credentials
            credentials of the database source

        Returns
        -------
            last tile index of the given tile_id and tile_type. Return -1 if record does not exist
        """
        if tile_type is None or tile_type.strip().upper() not in ["ONLINE", "OFFLINE"]:
            raise ValueError("tile_type must be either ONLINE or OFFLINE")

        tile_type = tile_type.strip().upper()
        tile_id = feature.tile_id
        data_source = ExtendedDatabaseSourceModel(**feature.datasource.dict())
        session = data_source.get_session(credentials=credentials)

        result = session.execute_query(
            f"SELECT LAST_TILE_INDEX_{tile_type} FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
        )
        if result is not None and len(result) > 0:
            return int(result["LAST_TILE_INDEX"].iloc[0])
        else:
            return -1
