"""
This module contains context related models.
"""
from typing import List, Optional

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph


class TabularDataToColumnNamesMapping(FeatureByteBaseModel):
    """
    Tabular data to the column names mapping
    """

    tabular_data_id: PydanticObjectId
    column_names: List[str]


class ContextModel(FeatureByteBaseDocumentModel):
    """
    Context is used to define the circumstances in which features are expected to be materialized.

    entity_ids: List[PydanticObjectId]
        List of entity ids associated with this context
    schema_at_inference: Optional[List[TabularDataToColumnNamesMapping]]
        Schema of the context at inference time
    graph: Optional[QueryGraph]
        Graph to store the context view
    """

    entity_ids: List[PydanticObjectId]
    schema_at_inference: Optional[List[TabularDataToColumnNamesMapping]]
    graph: Optional[QueryGraph]

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
