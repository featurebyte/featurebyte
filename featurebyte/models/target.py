"""
This module contains Target related models
"""
from __future__ import annotations

from typing import List, Optional

import pymongo
from bson import ObjectId
from pydantic import Field, validator

from featurebyte.common.validator import duration_string_validator
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node


class TargetModel(FeatureByteCatalogBaseDocumentModel):
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

    # Recipe
    graph: Optional[QueryGraph] = Field(allow_mutation=False)
    node_name: Optional[str] = Field(allow_mutation=False)

    # These fields will either be inferred from the recipe, or manually provided by the user only if they're creating
    # a target without a recipe.
    horizon: Optional[str]
    blind_spot: Optional[str]
    entity_ids: Optional[List[PydanticObjectId]] = Field(allow_mutation=False)

    target_namespace_id: PydanticObjectId = Field(allow_mutation=False, default_factory=ObjectId)

    # pydantic validators
    _duration_validator = validator("horizon", "blind_spot", pre=True, allow_reuse=True)(
        duration_string_validator
    )

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target"
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

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
            ],
        ]

    @property
    def node(self) -> Node:
        """
        Retrieve node

        Returns
        -------
        Node
            Node object
        """

        return self.graph.get_node_by_name(self.node_name)
