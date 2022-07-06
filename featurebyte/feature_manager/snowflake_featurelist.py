"""
Snowflake Feature List Manager class
"""
from __future__ import annotations

from typing import List, Tuple

from pydantic import BaseModel

from featurebyte.config import Credentials
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureListModel, TileSpec


class FeatureListManagerSnowflake(BaseModel):
    """
    Snowflake Feature Manager class

    feature_list: FeatureListModel
        feature instance
    credentials: Credentials
        credentials to the datasource
    """

    feature_list: FeatureListModel
    credentials: Credentials

    def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        pass
