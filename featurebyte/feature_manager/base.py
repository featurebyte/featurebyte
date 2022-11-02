"""
Snowflake Feature List Manager class
"""
from __future__ import annotations

from typing import Any, List, Tuple

from abc import abstractmethod

from pydantic import BaseModel, PrivateAttr

from featurebyte.models.tile import TileSpec
from featurebyte.session.base import BaseSession


class BaseFeatureListManager(BaseModel):
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

    @abstractmethod
    async def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        """
