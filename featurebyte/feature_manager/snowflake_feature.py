"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Any, List, Optional

import json

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_registry,
    tm_last_tile_index,
    tm_select_feature_registry,
    tm_update_feature_registry,
)
from featurebyte.logger import logger
from featurebyte.models.event_data import Feature
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileSnowflake


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
        data_source = ExtendedFeatureStoreModel(**self.feature.datasource.dict())
        self._session = data_source.get_session(credentials=self.credentials)

    def insert_feature_registry(self) -> bool:
        """
        Insert feature registry record. Update the is_default of the existing feature registry records to be False,
        then insert the new registry record with is_default to True

        Parameters
        ----------

        Returns
        -------
            whether the feature registry record is inserted successfully or not
        """
        feature_versions = self.retrieve_features(version=self.feature.version)
        logger.debug(f"feature_versions: {feature_versions}")
        if len(feature_versions) == 0:
            self._session.execute_query(
                tm_update_feature_registry.render(feature_name=self.feature.name, is_default=False)
            )
            logger.debug("Done updating is_default of other versions to false")
            tile_specs_lst = [tile_spec.dict() for tile_spec in self.feature.tile_specs]
            tile_specs_str = str(tile_specs_lst).replace("'", '"')
            sql = tm_insert_feature_registry.render(
                feature=self.feature, tile_specs_str=tile_specs_str
            )
            logger.debug(f"generated sql: {sql}")
            self._session.execute_query(sql)
            return True

        logger.debug(
            f"Feature version already exist for {self.feature.name} with version {self.feature.version}"
        )
        return False

    def retrieve_features(self, version: Optional[str] = None) -> List[Feature]:
        """
        Retrieve Feature instances. If version parameter is not presented, return all the feature versions

        Parameters
        ----------
        version: str
            version of Feature
        Returns
        -------
            list of Feature instances
        """
        sql = tm_select_feature_registry.render(feature_name=self.feature.name)
        if version:
            sql += f" AND VERSION = '{version}'"

        dataframe = self._session.execute_query(sql)
        result = []
        if dataframe is not None:
            for _, row in dataframe.iterrows():
                tile_specs = json.loads(row["TILE_SPECS"]) if row["TILE_SPECS"] else []
                feature_version = Feature(
                    name=self.feature.name,
                    version=row["VERSION"],
                    readiness=row["READINESS"],
                    is_default=row["IS_DEFAULT"],
                    tile_specs=tile_specs,
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
        for tile_spec in self.feature.tile_specs:
            logger.info(f"tile_spec: {tile_spec}")
            tile_mgr = TileSnowflake(
                time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
                blind_spot_seconds=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
                tile_sql=tile_spec.tile_sql,
                column_names=tile_spec.column_names,
                tile_id=tile_spec.tile_id,
                tabular_source=self.feature.datasource,
                credentials=self.credentials,
            )
            # insert tile_registry record
            tile_mgr.insert_tile_registry()
            logger.debug(f"Done insert_tile_registry for {tile_spec}")

            # enable online tiles scheduled job
            tile_mgr.schedule_online_tiles()
            logger.debug(f"Done schedule_online_tiles for {tile_spec}")

            # enable offline tiles scheduled job
            tile_mgr.schedule_offline_tiles()
            logger.debug(f"Done schedule_offline_tiles for {tile_spec}")

    def get_last_tile_index(self) -> pd.DataFrame:
        """
        Get last_tile_index of all the tile_ids as dataframe

        Returns
        -------
            last_tile_index of all the tile_ids as dataframe
        """
        sql = tm_last_tile_index.render(feature=self.feature)
        logger.debug(f"generated sql: {sql}")
        result = self._session.execute_query(sql)
        return result
