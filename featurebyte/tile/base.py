"""
Base class for Tile Management
"""
from __future__ import annotations

from pydantic import BaseModel, Field

from featurebyte.models.tile import TileSpec


class TileBase(BaseModel):
    """
    Snowflake Tile class

    tile_spec: TileSpec
        tile_spec for the tile manager class
    """

    tile_spec: TileSpec
