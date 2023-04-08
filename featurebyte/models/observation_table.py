"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

import pymongo
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

    location: TabularSource
        The table that stores the materialized data
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

    graph: QueryGraphModel
        The query graph that defines the view
    node_name: str
        The name of the node in the query graph that defines the view
    type: Literal[ObservationInputType.VIEW]
        The type of the input. Must be VIEW for this class
    """

    graph: QueryGraphModel
    node_name: StrictStr
    type: Literal[ObservationInputType.VIEW] = Field(ObservationInputType.VIEW, const=True)


class SourceTableObservationInput(FeatureByteBaseModel):
    """
    SourceTableObservationInput is the input for creating an ObservationTableModel from a source table

    source: TabularSource
        The source table
    type: Literal[ObservationInputType.SOURCE_TABLE]
        The type of the input. Must be SOURCE_TABLE for this class
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

    observation_input: ObservationInput
        The input that defines how the observation table is created
    context_id: Optional[PydanticObjectId]
        The id of the context that the observation table is associated with
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

        indexes = [
            pymongo.operations.IndexModel("user_id"),
            pymongo.operations.IndexModel("catalog_id"),
            pymongo.operations.IndexModel("name"),
            pymongo.operations.IndexModel("created_at"),
            pymongo.operations.IndexModel("updated_at"),
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
