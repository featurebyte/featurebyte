"""
Databricks Feature List Manager class
"""
from __future__ import annotations

from typing import Any, List, Tuple

from pydantic import PrivateAttr

from featurebyte.feature_manager.base import BaseFeatureListManager
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.databricks_tile import TileManagerDatabricks


class FeatureListManagerDatabricks(BaseFeatureListManager):
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
        super().__init__(session=session, **kw)

    async def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        """
        for tile_spec, entity_table in tile_inputs:
            tile_mgr = TileManagerDatabricks(
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
