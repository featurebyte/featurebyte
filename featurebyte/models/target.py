"""
This module contains Target related models
"""
from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph


class TargetModel(FeatureByteBaseDocumentModel):
    """
    Model for Target

    id: PydanticObjectId
        Target id of the object
    name: str
        Name of the Target
    created_at: datetime
        Datetime when the Target object was first saved or published
    updated_at: datetime
        Datetime when the Target object was last updated
    """

    description: Optional[str]
    # Recipe
    graph: QueryGraph = Field(allow_mutation=False)
    node_name: str

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "Target"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name", "user_id"),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
            ],
        ]
