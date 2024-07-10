"""
TileJobStatus model
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel, UniqueValuesConstraint
from featurebyte.models.tile import TileType


class TileJobLogModel(FeatureByteCatalogBaseDocumentModel):
    """
    TileJobStatusModel class

    The collection keeps track of the logs of tile jobs.
    """

    tile_id: StrictStr
    aggregation_id: StrictStr
    tile_type: TileType
    session_id: StrictStr
    status: StrictStr
    message: StrictStr
    traceback: Optional[StrictStr] = Field(default=None)
    duration: Optional[float] = Field(default=None)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "tile_job_log"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("tile_type"),
            pymongo.operations.IndexModel("aggregation_id"),
        ]
        auditable = False
