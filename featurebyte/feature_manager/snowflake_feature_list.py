"""
Snowflake Feature List Manager class
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.exception import DuplicatedRegistryError
from featurebyte.feature_manager.model import ExtendedFeatureListModel
from featurebyte.feature_manager.snowflake_sql_template import (
    tm_insert_feature_list_registry,
    tm_select_feature_list_registry,
    tm_update_feature_list_registry,
)
from featurebyte.logger import logger
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_tile import TileManagerSnowflake


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
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session

    async def insert_feature_list_registry(self, feature_list: ExtendedFeatureListModel) -> None:
        """
        Insert featurelist registry record. If the feature list record already exists, return False

        Parameters
        ----------
        feature_list: ExtendedFeatureListModel
            input featurelist instance

        Raises
        ----------
        DuplicatedRegistryError
            when the feature list registry record already exists
        """
        feature_list_versions = await self.retrieve_feature_list_registries(
            feature_list=feature_list, version=feature_list.version
        )

        logger.debug(f"feature_list_versions: {feature_list_versions}")
        if len(feature_list_versions) == 0:
            logger.debug(
                f"Inserting new FeatureList version for {feature_list.name} with version "
                f"{feature_list.version.to_str()}"
            )

            if feature_list.feature_signatures:
                feature_lst = [
                    {"feature": f.name, "version": f.version.to_str()}
                    for f in feature_list.feature_signatures
                ]
                feature_lst_str = str(feature_lst).replace("'", '"')
            else:
                feature_lst_str = "[]"

            sql = tm_insert_feature_list_registry.render(
                feature_list=feature_list, feature_lst_str=feature_lst_str
            )
            logger.debug(f"generated sql: {sql}")
            await self._session.execute_query(sql)
        else:
            raise DuplicatedRegistryError(
                f"FeatureList version already exist for {feature_list.name} with version "
                f"{feature_list.version.to_str()}"
            )

    async def retrieve_feature_list_registries(
        self, feature_list: FeatureListModel, version: Optional[VersionIdentifier] = None
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
                NAME, VERSION, READINESS, STATUS, FEATURE_VERSIONS, CREATED_AT
        """
        sql = tm_select_feature_list_registry.render(feature_list_name=feature_list.name)
        if version:
            sql += f" AND VERSION = '{version.to_str()}'"

        return await self._session.execute_query(sql)

    async def update_feature_list_registry(self, new_feature_list: FeatureListModel) -> None:
        """
        Update Feature List Registry record. Only readiness and status might be updated

        Parameters
        ----------
        new_feature_list: FeatureListModel
            new input feature instance

        Raises
        ----------
        ValueError
            when the feature registry record does not exist
        """
        feature_list_versions = await self.retrieve_feature_list_registries(
            feature_list=new_feature_list, version=new_feature_list.version
        )
        if len(feature_list_versions) == 0:
            raise ValueError(
                f"feature_list {new_feature_list.name} with version "
                f"{new_feature_list.version.to_str()} does not exist"
            )
        logger.debug(f"feature_list_versions: {feature_list_versions}")

        await self._session.execute_query(
            tm_update_feature_list_registry.render(feature_list=new_feature_list)
        )

    async def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        """
        for tile_spec, entity_table in tile_inputs:
            tile_mgr = TileManagerSnowflake(
                session=self._session,
            )

            await tile_mgr.generate_tiles(
                tile_spec=tile_spec, tile_type=TileType.OFFLINE, start_ts_str=None, end_ts_str=None
            )
            logger.debug(f"Done generating tiles for {tile_spec}")

            await tile_mgr.update_tile_entity_tracker(
                tile_spec=tile_spec, temp_entity_table=entity_table
            )
            logger.debug(f"Done update_tile_entity_tracker for {tile_spec}")
