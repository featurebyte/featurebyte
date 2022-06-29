"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Any, List, Optional

from jinja2 import Template
from pydantic import BaseModel, PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.logger import logger
from featurebyte.models.event_data import EventDataStatus, Feature
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileSnowflake

tm_ins_feature_registry = Template(
    """
    INSERT INTO FEATURE_REGISTRY (
        NAME, VERSION, STATUS, FREQUENCY_MINUTES, TIME_MODULO_FREQUENCY_SECOND,
        BLIND_SPOT_SECOND, TILE_SQL, COLUMN_NAMES, TILE_ID
    )
    VALUES (
        '{{feature.name}}', '{{feature.version}}', '{{feature.status.value}}', {{feature.frequency_minute}}, {{feature.time_modulo_frequency_second}},
        {{feature.blind_spot_second}}, '{{feature.tile_sql}}', '{{feature.column_names}}', '{{feature.tile_id}}'
    )
"""
)


class FeatureSnowflake(BaseModel):
    """
    Snowflake Feature Manager class

    Parameters
    ----------
    feature: Feature
        feature instance
    credentials: Credentials
        credentials to the datasource
    """

    feature: Feature
    credentials: Credentials
    _session: BaseSession = PrivateAttr()

    def __init__(self, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session with credentials

        Parameters
        ----------
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        data_source = ExtendedDatabaseSourceModel(**self.feature.datasource.dict())
        self._session = data_source.get_session(credentials=self.credentials)

    def insert_feature_registry(self) -> None:
        """
        Insert feature registry record. Update the is_default of the existing feature registry records to be False,
        then insert the new registry record with is_default to True

        Raises
        -------
        ValueError
            when there is an existing feature registry record with the same name and version
        """
        feature_versions = self.retrieve_features(version=self.feature.version)
        if len(feature_versions) == 0:
            self._session.execute_query(
                "UPDATE FEATURE_REGISTRY SET IS_DEFAULT = False WHERE NAME = '{self.feature.name}'"
            )
            logger.debug("Done updating is_default of other versions to false")
            insert_sql = tm_ins_feature_registry.render(feature=self.feature)
            logger.debug(f"insert_sql: {insert_sql}")
            self._session.execute_query(insert_sql)
        else:
            raise ValueError(
                f"Feature {self.feature.name} with version {self.feature.version} already exists"
            )

    def update_feature_registry(
        self, status: Optional[EventDataStatus] = None, online_enabled: Optional[bool] = None
    ) -> None:
        """
        update feature registry record

        Parameters
        ----------
        status: Optional[EventDataStatus]
            record's status to be updated
        online_enabled: Optional[bool]
            record's online_enabled to be updated
        """

    def retrieve_features(self, version: Optional[str] = None) -> List[Feature]:
        """
        Retrieve Feature instances. If version parameter is not passed, return all the feature versions

        Parameters
        ----------
        version: str
            version of Feature

        Returns
        -------
            list of Feature instances
        """
        sql = f" SELECT * FROM FEATURE_REGISTRY WHERE NAME = '{self.feature.name}' "
        if version:
            sql += f" AND VERSION = '{version}' "

        dataframe = self._session.execute_query(sql)
        result = []
        if dataframe is not None:
            for _, row in dataframe.iterrows():
                feature_version = Feature(
                    name=self.feature.name,
                    version=row["VERSION"],
                    status=row["STATUS"],
                    is_default=row["IS_DEFAULT"],
                    time_modulo_frequency_second=row["TIME_MODULO_FREQUENCY_SECOND"],
                    blind_spot_second=row["BLIND_SPOT_SECOND"],
                    frequency_minute=row["FREQUENCY_MINUTES"],
                    tile_sql=row["TILE_SQL"],
                    column_names=row["COLUMN_NAMES"],
                    tile_id=row["TILE_ID"],
                    online_enabled=row["ONLINE_ENABLED"],
                    datasource=self.feature.datasource,
                )
                result.append(feature_version)

        return result

    def online_enable(self) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        """
        tile_mgr = TileSnowflake(
            feature_name=self.feature.name,
            time_modulo_frequency_seconds=self.feature.time_modulo_frequency_second,
            blind_spot_seconds=self.feature.blind_spot_second,
            frequency_minute=self.feature.frequency_minute,
            tile_sql=self.feature.tile_sql,
            column_names=self.feature.column_names,
            tile_id=self.feature.tile_id,
            tabular_source=self.feature.datasource,
            credentials=self.credentials,
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

    def get_last_tile_index(self, tile_type: str) -> int:
        """
        Get last_tile_index status of a tile_id

        Parameters
        ----------
        tile_type: str
            tile type. ie. ONLINE or OFFLINE

        Raises
        ----------
        ValueError
            if tile_type is not ONLINE or OFFLINE

        Returns
        -------
            last tile index of the given tile_id and tile_type. Return -1 if record does not exist
        """
        if tile_type is None or tile_type.strip().upper() not in ["ONLINE", "OFFLINE"]:
            raise ValueError("tile_type must be either ONLINE or OFFLINE")

        tile_type = tile_type.strip().upper()
        tile_id = self.feature.tile_id

        result = self._session.execute_query(
            f"SELECT LAST_TILE_INDEX_{tile_type} FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
        )
        if result is not None and len(result) > 0:
            return int(result["LAST_TILE_INDEX"].iloc[0])

        return -1
