"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_registry,
    tm_last_tile_index,
    tm_select_feature_registry,
    tm_update_feature_registry,
)
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileSnowflake


class FeatureManagerSnowflake(BaseModel):
    """
    Snowflake Feature Manager class

    credentials: Credentials
        credentials to the datasource
    """

    credentials: Credentials
    _session: BaseSession = PrivateAttr(default=None)

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
        session = self._get_session(feature)
        feature_versions = self.retrieve_feature_registries(
            feature=feature, version=feature.version
        )

        logger.debug(f"feature_versions: {feature_versions}")
        if len(feature_versions) == 0:
            session.execute_query(
                tm_update_feature_registry.render(feature_name=feature.name, is_default=False)
            )
            logger.debug("Done updating is_default of other versions to false")

            if feature.tile_specs:
                tile_specs_lst = [tile_spec.dict() for tile_spec in feature.tile_specs]
                tile_specs_str = str(tile_specs_lst).replace("'", '"')
            else:
                tile_specs_str = "[]"

            sql = tm_insert_feature_registry.render(feature=feature, tile_specs_str=tile_specs_str)
            logger.debug(f"generated sql: {sql}")
            session.execute_query(sql)
            return True

        logger.debug(
            f"Feature version already exist for {feature.name} with version {feature.version}"
        )
        return False

    def retrieve_feature_registries(
        self, feature: FeatureModel, version: Optional[str] = None
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

        return self._get_session(feature).execute_query(sql)

    def update_feature_registry(self, feature: FeatureModel) -> None:
        """
        Update Feature Registry record

        Parameters
        ----------
        feature: FeatureModel
            input feature instance
        """
        feature_versions = self.retrieve_feature_registries(
            feature=feature, version=feature.version
        )
        if len(feature_versions) == 0:
            raise ValueError(
                f"feature {feature.name} with version {feature.version} does not exist"
            )

        logger.debug(f"feature_versions: {feature_versions}")
        # TODO: update sql

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
                tile_mgr = TileSnowflake(
                    time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
                    blind_spot_seconds=tile_spec.blind_spot_second,
                    frequency_minute=tile_spec.frequency_minute,
                    tile_sql=tile_spec.tile_sql,
                    column_names=tile_spec.column_names,
                    tile_id=tile_spec.tile_id,
                    tabular_source=feature.tabular_source[0],
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
        result = self._get_session(feature).execute_query(sql)
        return result

    def _get_session(self, feature: FeatureModel) -> BaseSession:
        """
        Helper method to get the datasource session from FeatureModel

        Parameters
        ----------
        feature: FeatureModel
            input feature instance

        Returns
        -------
            database session
        """
        if not self._session:
            feature_store = ExtendedFeatureStoreModel(**feature.tabular_source[0].dict())
            self._session = feature_store.get_session(credentials=self.credentials)
        return self._session
