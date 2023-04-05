"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import List, Optional, Union

from pydantic import Field, StrictStr

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel


class MaterializedTable(FeatureByteCatalogBaseDocumentModel):
    """
    MaterializedTable represents a table that has been materialized and stored in feature store
    database.
    """

    name: StrictStr
    location: TabularSource


class ObservationInputType(StrEnum):
    """
    Input type refers to how an ObservationTableModel is created
    """

    VIEW = "view"
    SOURCE_TABLE = "source_table"


class ViewObservationInput(FeatureByteBaseModel):
    """
    ViewObservationInput is the input for creating an ObservationTableModel from a view
    """

    graph: QueryGraphModel
    node_name: StrictStr


class SourceTableObservationInput(FeatureByteBaseModel):
    """
    SourceTableObservationInput is the input for creating an ObservationTableModel from a source table
    """

    source: TabularSource


ObservationInput = Union[ViewObservationInput, SourceTableObservationInput]


class ObservationTableModel(MaterializedTable):
    """
    ObservationTableModel is a table that can be used to request historical features
    """

    observation_input: ObservationInput
    column_names: List[StrictStr]
    context_id: Optional[PydanticObjectId] = Field(default=None)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "observation_table"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
