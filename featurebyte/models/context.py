"""
This module contains context related models.
"""
from typing import List, Optional

import pymongo

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph


class ContextModel(FeatureByteCatalogBaseDocumentModel):
    """
    Context is used to define the circumstances in which features are expected to be materialized.

    entity_ids: List[PydanticObjectId]
        List of entity ids associated with this context
    graph: Optional[QueryGraph]
        Graph to store the context view
    """

    entity_ids: List[PydanticObjectId]
    graph: Optional[QueryGraph]
    node_name: Optional[str]

    class Settings:
        """
        MongoDB Settings
        """

        collection_name: str = "context"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = [
            pymongo.operations.IndexModel("user_id"),
            pymongo.operations.IndexModel("catalog_id"),
            pymongo.operations.IndexModel("name"),
            pymongo.operations.IndexModel("created_at"),
            pymongo.operations.IndexModel("updated_at"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
