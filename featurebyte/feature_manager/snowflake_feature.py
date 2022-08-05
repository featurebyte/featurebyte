"""
Snowflake Feature Manager class
"""
from __future__ import annotations

from typing import Any, Optional

import json

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.exception import (
    DuplicatedFeatureRegistryError,
    InvalidFeatureRegistryOperationError,
    MissingFeatureRegistryError,
)
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_feature_tile_monitor,
    tm_insert_feature_registry,
    tm_last_tile_index,
    tm_remove_feature_registry,
    tm_select_feature_registry,
    tm_update_feature_registry,
    tm_update_feature_registry_default_false,
)
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureReadiness, FeatureVersionIdentifier
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

    def insert_feature_registry(self, feature: ExtendedFeatureModel) -> None:
        """
        Insert feature registry record. Update the is_default of the existing feature registry records to be False,
        then insert the new registry record with is_default to True

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance

        Raises
        ----------
        DuplicatedFeatureRegistryError
            when the feature registry record already exists
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
                tile_specs_str = json.dumps(tile_specs_lst)
                # This JSON encoded string will be embedded in Snowflake SQL, which has its own
                # escape sequence, so we need to escape one more time
                tile_specs_str = tile_specs_str.replace("\\", "\\\\")
                tile_specs_str = tile_specs_str.replace("'", "\\'")
            else:
                tile_specs_str = "[]"

            event_ids = [str(e_id) for e_id in feature.event_data_ids]
            sql = tm_insert_feature_registry.render(
                feature=feature,
                tile_specs_str=tile_specs_str,
                event_ids_str=",".join(event_ids),
            )
            logger.debug(f"generated insert sql: {sql}")
            self._session.execute_query(sql)
        else:
            raise DuplicatedFeatureRegistryError(
                f"Feature version already exist for {feature.name} with version {feature.version}"
            )

    def remove_feature_registry(self, feature: ExtendedFeatureModel) -> None:
        """
        Remove the feature registry record

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance

        Raises
        ----------
        MissingFeatureRegistryError
            when the feature registry record does not exist
        InvalidFeatureRegistryOperationError
            when the readiness of the feature is not DRAFT
        """
        feature_versions = self.retrieve_feature_registries(
            feature=feature, version=feature.version
        )

        logger.debug(f"feature_versions: {feature_versions}")
        if len(feature_versions) == 0:
            raise MissingFeatureRegistryError(
                f"Feature version does not exist for {feature.name} with version {feature.version}"
            )

        feature_readiness = feature_versions["READINESS"].iloc[0]
        if feature_readiness != "DRAFT":
            raise InvalidFeatureRegistryOperationError(
                f"Feature version {feature.name} with version {feature.version} cannot be deleted with readiness "
                f"{feature_readiness}"
            )

        sql = tm_remove_feature_registry.render(feature=feature)
        logger.debug(f"generated remove sql: {sql}")
        self._session.execute_query(sql)
        logger.debug(f"Done removing feature version {feature.name} with version {feature.version}")

    def retrieve_feature_registries(
        self, feature: ExtendedFeatureModel, version: Optional[FeatureVersionIdentifier] = None
    ) -> pd.DataFrame:
        """
        Retrieve Feature instances. If version parameter is not presented, return all the feature versions.
        It will retrieve the rows from table FEATURE_REGISTRY as DataFrame

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance
        version: str
            version of Feature

        Returns
        -------
        pd.DataFrame
            dataframe of the FEATURE_REGISTRY rows with the following columns:
                NAME, VERSION, READINESS, TILE_SPECS, IS_DEFAULT, ONLINE_ENABLED, CREATED_AT
        """
        sql = tm_select_feature_registry.render(feature_name=feature.name, version=version)

        logger.debug(f"select sql: {sql}")
        return self._session.execute_query(sql)

    def update_feature_registry(
        self, new_feature: ExtendedFeatureModel, to_online_enable: bool
    ) -> None:
        """
        Update Feature Registry record. Only readiness, description and is_default might be updated

        Parameters
        ----------
        new_feature: ExtendedFeatureModel
            new input feature instance
        to_online_enable: bool
            whether to make the feature online enabled

        Raises
        ----------
        MissingFeatureRegistryError
            when the feature registry record does not exist
        """
        feature_versions = self.retrieve_feature_registries(
            feature=new_feature, version=new_feature.version
        )
        if len(feature_versions) == 0:
            raise MissingFeatureRegistryError(
                f"feature {new_feature.name} with version {new_feature.version} does not exist"
            )
        logger.debug(f"feature_versions: {feature_versions}")

        update_sql = tm_update_feature_registry.render(
            feature=new_feature, online_enabled=to_online_enable
        )
        logger.debug(f"update_sql: {update_sql}")
        self._session.execute_query(update_sql)

    def online_enable(self, feature: ExtendedFeatureModel) -> None:
        """
        Schedule both online and offline tile jobs

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance

        Raises
        ----------
        InvalidFeatureRegistryOperationError
            when the input feature readiness is not PRODUCTION_READY
            when the feature registry record is already online enabled
        MissingFeatureRegistryError
            when the feature registry record does not exist
        """
        if feature.tile_specs:
            if feature.readiness is None or feature.readiness != FeatureReadiness.PRODUCTION_READY:
                raise InvalidFeatureRegistryOperationError(
                    "feature readiness has to be PRODUCTION_READY before online_enable"
                )

            # check whether the feature exists
            feature_versions = self.retrieve_feature_registries(
                feature=feature, version=feature.version
            )

            if len(feature_versions) == 0:
                raise MissingFeatureRegistryError(
                    f"feature {feature.name} with version {feature.version} does not exist"
                )

            if feature_versions["ONLINE_ENABLED"].iloc[0]:
                raise InvalidFeatureRegistryOperationError(
                    f"feature {feature.name} with version {feature.version} is already online enabled"
                )

            # insert Tile Specs
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

            # update ONLINE_ENABLED of the feature registry record to True
            self.update_feature_registry(feature, to_online_enable=True)

    def retrieve_last_tile_index(self, feature: ExtendedFeatureModel) -> pd.DataFrame:
        """
        Get last_tile_index of all the tile_ids as dataframe

        Parameters
        ----------
        feature: ExtendedFeatureModel
            input feature instance

        Returns
        -------
            last_tile_index of all the tile_ids as dataframe
        """
        sql = tm_last_tile_index.render(feature=feature)
        logger.debug(f"generated sql: {sql}")
        result = self._session.execute_query(sql)
        return result

    def retrieve_feature_tile_inconsistency_data(
        self, query_start_ts: str, query_end_ts: str
    ) -> pd.DataFrame:
        """
        Retrieve the raw data of feature tile inconsistency monitoring

        Parameters
        ----------
        query_start_ts: str
            start monitoring timestamp of tile inconsistency
        query_end_ts: str
            end monitoring timestamp of tile inconsistency

        Returns
        -------
            raw data of feature-tile inconsistency as dataframe
        """
        sql = tm_feature_tile_monitor.render(
            query_start_ts=query_start_ts, query_end_ts=query_end_ts
        )
        logger.debug(f"generated sql: {sql}")
        result = self._session.execute_query(sql)
        return result
