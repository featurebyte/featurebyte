"""
ObservationTableModel models
"""
from __future__ import annotations

from typing import List, Literal, Optional, Union
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

import pymongo
from pydantic import Field, StrictStr

from featurebyte.enum import SourceType, StrEnum
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import (
    get_materialise_from_source_sql,
    get_materialise_from_view_sql,
)


class MaterializedTable(FeatureByteCatalogBaseDocumentModel):
    """
    MaterializedTable represents a table that has been materialized and stored in feature store
    database.

    location: TabularSource
        The table that stores the materialized data
    """

    location: TabularSource

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("location.feature_store_id"),
        ]


class ObservationInputType(StrEnum):
    """
    Input type refers to how an ObservationTableModel is created
    """

    VIEW = "view"
    SOURCE_TABLE = "source_table"


class BaseObservationInput(FeatureByteBaseModel):
    """
    BaseObservationInput is the base class for all ObservationInput types
    """

    @abstractmethod
    def get_materialise_sql(self, destination: TableDetails, source_type: SourceType) -> str:
        """
        Get the SQL statement that materializes the observation table

        Parameters
        ----------
        destination: TableDetails
            The destination table
        source_type: SourceType
            The source type of the destination table

        Returns
        -------
        str
        """


class ViewObservationInput(BaseObservationInput):
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

    def get_materialise_sql(self, destination: TableDetails, source_type: SourceType) -> str:
        return get_materialise_from_view_sql(
            graph=self.graph,
            node_name=self.node_name,
            destination=destination,
            source_type=source_type,
        )


class SourceTableObservationInput(BaseObservationInput):
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

    def get_materialise_sql(self, destination: TableDetails, source_type: SourceType) -> str:
        return get_materialise_from_source_sql(
            source=self.source.table_details,
            destination=destination,
            source_type=source_type,
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

    class Settings(MaterializedTable.Settings):
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

        indexes = MaterializedTable.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
