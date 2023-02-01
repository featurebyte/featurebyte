"""
This module contains DatabaseSource related models
"""
from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

from abc import ABC, abstractmethod

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType, OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import BaseTableData, FrozenTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.schema import FeatureStoreDetails


class FeatureStoreModel(FeatureByteBaseDocumentModel, FeatureStoreDetails):
    """Model for a feature store"""

    name: StrictStr

    def get_feature_store_details(self) -> FeatureStoreDetails:
        """
        Get feature store details

        Returns
        -------
        FeatureStoreDetails
        """
        return FeatureStoreDetails(**self.json_dict())

    class Settings:
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


class DataStatus(OrderedStrEnum):
    """Data status"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
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
            Serialized table data dictionary
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


class FrozenDataModel(FrozenTableData, ConstructGraphMixin, FeatureByteBaseDocumentModel, ABC):
    """
    FrozenDataModel stores all the attributes that are fixed after object construction.
    """

    _table_data_class: ClassVar[Type[BaseTableData]] = BaseTableData  # type: ignore[misc]

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "tabular_data"
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
            ),
        ]


class DataModel(BaseTableData, FrozenDataModel, ABC):
    """
    DataModel schema

    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event data columns
    status: DataStatus
        Data status
    record_creation_date_column: Optional[str]
        Record creation date column name
    """

    status: DataStatus = Field(default=DataStatus.DRAFT, allow_mutation=False)
    record_creation_date_column: Optional[StrictStr]

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the data model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.entity_id for col in self.columns_info if col.entity_id))

    @property
    def semantic_ids(self) -> List[PydanticObjectId]:
        """
        List of semantic IDs in the data model

        Returns
        -------
        List[PydanticObjectId]
        """
        return list(set(col.semantic_id for col in self.columns_info if col.semantic_id))

    @property
    @abstractmethod
    def primary_key_columns(self) -> List[str]:
        """
        Primary key column names

        Returns
        -------
        List[str]
        """

    @classmethod
    def validate_column_exists(
        cls,
        column_name: Optional[str],
        values: dict[str, Any],
        expected_types: Optional[set[DBVarType]],
    ) -> Optional[str]:
        """
        Validate whether the column name exists in the columns info

        Parameters
        ----------
        column_name: Optional[str]
            Column name value
        values: dict[str, Any]
            Input dictionary to data model
        expected_types: Optional[set[DBVarType]]
            Expected column types

        Returns
        -------
        Optional[str]

        Raises
        ------
        ValueError
            If the column name does not exist in columns_info
        """
        if column_name is not None:
            matched_col_dict = None
            for col in values["columns_info"]:
                col_dict = dict(col)
                if col_dict["name"] == column_name:
                    matched_col_dict = col_dict
                    break

            if matched_col_dict is None:
                raise ValueError(f'Column "{column_name}" not found in the table!')
            if expected_types and matched_col_dict.get("dtype") not in expected_types:
                dtypes = sorted(str(dtype) for dtype in expected_types)
                raise ValueError(f'Column "{column_name}" is expected to have type(s): {dtypes}')
        return column_name
