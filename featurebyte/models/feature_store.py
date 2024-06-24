"""
This module contains DatabaseSource related models
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

from abc import ABC, abstractmethod

import pymongo
from pydantic import Field, StrictStr

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteCatalogBaseDocumentModel,
    NameStr,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class FeatureStoreModel(FeatureByteBaseDocumentModel, FeatureStoreDetails):
    """Model for a feature store"""

    name: NameStr

    def get_feature_store_details(self) -> FeatureStoreDetails:
        """
        Get feature store details

        Returns
        -------
        FeatureStoreDetails
        """
        return FeatureStoreDetails(**self.dict(by_alias=True))

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_store"
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
            UniqueValuesConstraint(
                fields=("details",),
                conflict_fields_signature={"details": ["details"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("type"),
            pymongo.operations.IndexModel("details"),
            [
                ("name", pymongo.TEXT),
                ("type", pymongo.TEXT),
            ],
        ]


class TableStatus(OrderedStrEnum):
    """Table status"""

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.TableStatus")

    DEPRECATED = "DEPRECATED"
    PUBLIC_DRAFT = "PUBLIC_DRAFT"
    PUBLISHED = "PUBLISHED"


class ConstructGraphMixin:
    """ConstructGraphMixin class"""

    _table_data_class: ClassVar[Type[BaseTableData]] = BaseTableData  # type: ignore[misc]

    @classmethod
    def construct_graph_and_node(
        cls,
        feature_store_details: FeatureStoreDetails,
        table_data_dict: Dict[str, Any],
        graph: Optional[QueryGraph] = None,
    ) -> Tuple[QueryGraph, Node]:
        """
        Construct graph & node based on column info

        Parameters
        ----------
        feature_store_details: FeatureStoreDetails
            Feature store details
        table_data_dict: Dict[str, Any]
            Serialized table dictionary
        graph: Optional[QueryGraph]
            Graph object to insert the node or create a new QueryGraph object if the param is empty

        Returns
        -------
        Tuple[QueryGraph, Node]
        """
        if graph is None:
            graph = QueryGraph()

        table_data = cls._table_data_class(**table_data_dict)
        input_node = table_data.construct_input_node(  # pylint: disable=no-member
            feature_store_details=feature_store_details
        )
        inserted_input_node = graph.add_node(node=input_node, input_nodes=[])
        return graph, inserted_input_node


class TableModel(BaseTableData, ConstructGraphMixin, FeatureByteCatalogBaseDocumentModel, ABC):
    """
    TableModel schema

    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of table column information
    status: TableStatus
        Table status
    record_creation_timestamp_column: Optional[str]
        Record creation timestamp column name
    """

    status: TableStatus = Field(default=TableStatus.PUBLIC_DRAFT, allow_mutation=False)
    record_creation_timestamp_column: Optional[StrictStr]
    _table_data_class: ClassVar[Type[BaseTableData]] = BaseTableData  # type: ignore[misc]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the table model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.entity_id for col in self.columns_info if col.entity_id))

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the table model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.semantic_id for col in self.columns_info if col.semantic_id))

    @property
    def table_data(self) -> BaseTableData:
        """
        Table table

        Returns
        -------
        BaseTableData
        """
        return self._table_data_class(**self.dict(by_alias=True))

    @property
    def table_primary_key_entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs that are primary key in the table model

        Returns
        -------
        List[PydanticObjectId]
        """
        return [
            col.entity_id
            for col in self.columns_info
            if col.entity_id and col.name in self.primary_key_columns
        ]

    @property
    @abstractmethod
    def primary_key_columns(self) -> List[str]:
        """
        Primary key column names

        Returns
        -------
        List[str]
        """

    @property
    @abstractmethod
    def special_columns(self) -> List[str]:
        """
        Special columns is a list of columns that have special meaning in the table

        Returns
        -------
        List[str]
        """

    @abstractmethod
    def create_view_graph_node(
        self, input_node: InputNode, metadata: Any, **kwargs: Any
    ) -> Tuple[GraphNode, List[ColumnInfo]]:
        """
        Create view graph node

        Parameters
        ----------
        input_node: InputNode
            Input node
        metadata: Any
            Metadata add to the graph node
        kwargs: Any
            Additional arguments

        Returns
        -------
        Tuple[GraphNode, List[ColumnInfo]]
        """

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "table"
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
            UniqueValuesConstraint(
                fields=("tabular_source",),
                conflict_fields_signature={"tabular_source": ["tabular_source"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
                extra_query_params={"status": {"$ne": TableStatus.DEPRECATED.value}},
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("type"),
            pymongo.operations.IndexModel("status"),
            pymongo.operations.IndexModel("tabular_source.feature_store_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
