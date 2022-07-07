"""
Snowflake Feature List Manager class
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_list_registry,
    tm_select_feature_list_registry,
    tm_update_feature_list_registry,
)
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureListModel, FeatureListVersionIdentifier
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileSnowflake


class FeatureListManagerSnowflake(BaseModel):
    """
    Snowflake Feature Manager class
    """

    _session: BaseSession = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session

    def insert_feature_list_registry(self, feature_list: FeatureListModel) -> bool:
        """
        Insert featurelist registry record. If the feature list record already exists, return False

        Parameters
        ----------
        feature_list: FeatureListModel
            input featurelist instance

        Returns
        -------
            whether the featurelist registry record is inserted successfully or not
        """
        feature_list_versions = self.retrieve_feature_list_registries(
            feature_list=feature_list, version=feature_list.version
        )

        logger.debug(f"feature_list_versions: {feature_list_versions}")
        if len(feature_list_versions) == 0:
            logger.debug(
                f"Inserting new FeatureList version for {feature_list.name} with version {feature_list.version}"
            )

            if feature_list.features:
                feature_lst = [{"feature": f[0], "version": f[1]} for f in feature_list.features]
                feature_lst_str = str(feature_lst).replace("'", '"')
            else:
                feature_lst_str = "[]"

            sql = tm_insert_feature_list_registry.render(
                feature_list=feature_list, feature_lst_str=feature_lst_str
            )
            logger.debug(f"generated sql: {sql}")
            self._session.execute_query(sql)
            return True

        logger.debug(
            f"FeatureList version already exist for {feature_list.name} with version {feature_list.version}"
        )
        return False

    def retrieve_feature_list_registries(
        self, feature_list: FeatureListModel, version: Optional[FeatureListVersionIdentifier] = None
    ) -> pd.DataFrame:
        """
        Retrieve FeatureList instances. If version parameter is not presented, return all the FeatureList versions.
        It will retrieve the rows from table FEATURE_LIST_REGISTRY as DataFrame

        Parameters
        ----------
        feature_list: FeatureListModel
            input feature instance
        version: str
            version of Feature
        Returns
        -------
            dataframe of the FEATURE_REGISTRY rows with the following columns:
                NAME, VERSION, DESCRIPTION, READINESS, STATUS, FEATURE_VERSIONS, CREATED_AT
        """
        sql = tm_select_feature_list_registry.render(feature_list_name=feature_list.name)
        if version:
            sql += f" AND VERSION = '{version}'"

        return self._session.execute_query(sql)

    def update_feature_list_registry(
        self, feature_list: FeatureListModel, attribute_name: str, attribute_value: str
    ) -> None:
        """
        Update Feature Registry record

        Parameters
        ----------
        feature_list: FeatureListModel
            input feature instance
        attribute_name: str
            attribute/column name
        attribute_value: str
            attribute/column value
        """
        feature_list_versions = self.retrieve_feature_list_registries(
            feature_list=feature_list, version=feature_list.version
        )
        if len(feature_list_versions) == 0:
            raise ValueError(
                f"feature_list {feature_list.name} with version {feature_list.version} does not exist"
            )
        logger.debug(f"feature_list_versions: {feature_list_versions}")

        self._session.execute_query(
            tm_update_feature_list_registry.render(
                feature_list_name=feature_list.name,
                col_name=attribute_name,
                col_value=f"'{attribute_value}'",
            )
        )

    def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs:

        Returns
        -------

        """
        for tile_spec, entity_table in tile_inputs:
            tile_mgr = TileSnowflake(
                tile_spec=tile_spec,
                session=self._session,
            )

            tile_mgr.generate_tiles(tile_type=TileType.OFFLINE, start_ts_str=None, end_ts_str=None)
            logger.debug(f"Done generating tiles for {tile_spec}")
