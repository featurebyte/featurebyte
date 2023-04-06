"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import Annotated, List, Literal, Optional, Union

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
    type: Literal[ObservationInputType.VIEW] = Field(ObservationInputType.VIEW, const=True)


class SourceTableObservationInput(FeatureByteBaseModel):
    """
    SourceTableObservationInput is the input for creating an ObservationTableModel from a source table
    """

    source: TabularSource
    type: Literal[ObservationInputType.SOURCE_TABLE] = Field(
        ObservationInputType.SOURCE_TABLE, const=True
    )


ObservationInput = Annotated[
    Union[ViewObservationInput, SourceTableObservationInput], Field(discriminator="type")
]


class ObservationTableModel(MaterializedTable):
    """
    ObservationTableModel is a table that can be used to request historical features
    """

    observation_input: ObservationInput
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
