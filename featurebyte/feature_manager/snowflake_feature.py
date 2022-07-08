"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_registry,
    tm_last_tile_index,
    tm_select_feature_registry,
    tm_update_feature_registry,
    tm_update_feature_registry_default_false,
)
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel, FeatureVersionIdentifier
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileManagerSnowflake


class FeatureManagerSnowflake(BaseModel):
    """
    Snowflake Feature Manager class
    """

    _session: BaseSession = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session

    def insert_feature_registry(self, feature: FeatureModel) -> bool:
        """
        Insert feature registry record. Update the is_default of the existing feature registry records to be False,
        then insert the new registry record with is_default to True

        Parameters
        ----------
        feature: FeatureModel
            input feature instance

        Returns
        -------
            whether the feature registry record is inserted successfully or not
        """
        feature_versions = self.retrieve_feature_registries(
            feature=feature, version=feature.version
        )

        logger.debug(f"feature_versions: {feature_versions}")
        if len(feature_versions) == 0:
            update_sql = tm_update_feature_registry_default_false.render(feature=feature)
            self._session.execute_query(update_sql)
            logger.debug("Done updating is_default of other versions to false")

            if feature.tile_specs:
                tile_specs_lst = [tile_spec.dict() for tile_spec in feature.tile_specs]
                tile_specs_str = str(tile_specs_lst).replace("'", '"')
            else:
                tile_specs_str = "[]"

            sql = tm_insert_feature_registry.render(feature=feature, tile_specs_str=tile_specs_str)
            logger.debug(f"generated sql: {sql}")
            self._session.execute_query(sql)
            return True

        logger.debug(
            f"Feature version already exist for {feature.name} with version {feature.version}"
        )
        return False

    def retrieve_feature_registries(
        self, feature: FeatureModel, version: Optional[FeatureVersionIdentifier] = None
    ) -> pd.DataFrame:
        """
        Retrieve Feature instances. If version parameter is not presented, return all the feature versions.
        It will retrieve the rows from table FEATURE_REGISTRY as DataFrame

        Parameters
        ----------
        feature: FeatureModel
            input feature instance
        version: str
            version of Feature
        Returns
        -------
            dataframe of the FEATURE_REGISTRY rows with the following columns:
                NAME, VERSION, READINESS, TILE_SPECS, IS_DEFAULT, ONLINE_ENABLED, CREATED_AT
        """
        sql = tm_select_feature_registry.render(feature_name=feature.name)
        if version:
            sql += f" AND VERSION = '{version}'"

        return self._session.execute_query(sql)

    def update_feature_registry(self, new_feature: FeatureModel) -> None:
        """
        Update Feature Registry record. Only readiness, description and is_default might be updated

        Parameters
        ----------
        new_feature: FeatureModel
            new input feature instance

        Raises
        ----------
        ValueError
            when the feature registry record does not exist
        """
        feature_versions = self.retrieve_feature_registries(
            feature=new_feature, version=new_feature.version
        )
        if len(feature_versions) == 0:
            raise ValueError(
                f"feature {new_feature.name} with version {new_feature.version} does not exist"
            )
        logger.debug(f"feature_versions: {feature_versions}")

        update_sql = tm_update_feature_registry.render(feature=new_feature)
        logger.debug(f"update_sql: {update_sql}")
        self._session.execute_query(update_sql)

    def online_enable(self, feature: FeatureModel) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        feature: FeatureModel
            input feature instance
        """
        if feature.tile_specs:
            for tile_spec in feature.tile_specs:
                logger.info(f"tile_spec: {tile_spec}")
                tile_mgr = TileManagerSnowflake(
                    session=self._session,
                )
                # insert tile_registry record
                tile_mgr.insert_tile_registry(tile_spec=tile_spec)
                logger.debug(f"Done insert_tile_registry for {tile_spec}")

                # enable online tiles scheduled job
                tile_mgr.schedule_online_tiles(tile_spec=tile_spec)
                logger.debug(f"Done schedule_online_tiles for {tile_spec}")

                # enable offline tiles scheduled job
                tile_mgr.schedule_offline_tiles(tile_spec=tile_spec)
                logger.debug(f"Done schedule_offline_tiles for {tile_spec}")

    def get_last_tile_index(self, feature: FeatureModel) -> pd.DataFrame:
        """
        Get last_tile_index of all the tile_ids as dataframe

        Parameters
        ----------
        feature: FeatureModel
            input feature instance

        Returns
        -------
            last_tile_index of all the tile_ids as dataframe
        """
        sql = tm_last_tile_index.render(feature=feature)
        logger.debug(f"generated sql: {sql}")
        result = self._session.execute_query(sql)
        return result
