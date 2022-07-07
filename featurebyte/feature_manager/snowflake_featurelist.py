"""
Snowflake Feature List Manager class
"""
from __future__ import annotations

from typing import Any, List, Tuple

from pydantic import BaseModel, PrivateAttr

from featurebyte.logger import logger
from featurebyte.models.feature import FeatureListModel, TileSpec
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

    def insert_featurelist_registry(self, feature: FeatureListModel) -> bool:
        pass

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
                time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
                blind_spot_seconds=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
                tile_sql=tile_spec.tile_sql,
                column_names=tile_spec.column_names,
                tile_id=tile_spec.tile_id,
                session=self._session,
            )
