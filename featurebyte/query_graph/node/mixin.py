"""
This module contains mixins used in node classes
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Set

from pydantic import Field

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.agg_func import construct_agg_func
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.column import InColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureInfo,
    ViewDataColumn,
)


class BaseGroupbyParameters(FeatureByteBaseModel):
    """Common parameters related to groupby operation"""

    keys: List[InColumnStr]
    parent: Optional[InColumnStr] = Field(default=None)
    agg_func: AggFunc
    value_by: Optional[InColumnStr] = Field(default=None)
    serving_names: List[str]
    entity_ids: Optional[List[PydanticObjectId]] = Field(default=None)


class AggregationOpStructMixin(BaseNode, ABC):
    """AggregationOpStructMixin class"""

    @abstractmethod
    def _get_aggregations(
        self,
        columns: List[ViewDataColumn],
        node_name: str,
        other_node_names: Set[str],
        output_dtype_info: DBVarTypeInfo,
    ) -> List[AggregationColumn]:
        """
        Construct aggregations based on node parameters

        Parameters
        ----------
        columns: List[ViewDataColumn]
            Input columns
        node_name: str
            Node name
        other_node_names: Set[str]
            Set of node names (input lineage)
        output_dtype_info: DBVarTypeInfo
            Aggregation output variable type

        Returns
        -------
        List of aggregation columns
        """

    @abstractmethod
    def _exclude_source_columns(self) -> List[str]:
        """
        Exclude column to be include in source columns

        Returns
        -------
        List of excluded column names
        """

    @abstractmethod
    def _is_time_based(self) -> bool:
        """
        Returns whether the aggregation is time based

        Returns
        -------
        bool
        """

    def _get_parent_columns(self, columns: List[ViewDataColumn]) -> Optional[List[ViewDataColumn]]:
        """
        Get the table column used for aggregation

        Parameters
        ----------
        columns: List[ViewDataColumn]
            List of input columns

        Returns
        -------
        Optional[ViewDataColumn]
        """
        parent_columns = None
        if isinstance(self.parameters, BaseGroupbyParameters) and self.parameters.parent:
            parent_column = next(
                (col for col in columns if col.name == self.parameters.parent), None
            )
            if parent_column:
                parent_columns = [parent_column]
        return parent_columns

    def _get_agg_func(self) -> Optional[AggFunc]:
        """
        Retrieve agg_func from the parameters

        Returns
        -------
        Optional[str]
        """
        if isinstance(self.parameters, BaseGroupbyParameters):
            return self.parameters.agg_func
        return None

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        _ = global_state
        input_operation_info = inputs[0]
        required_input_columns = self.get_required_input_columns(
            input_index=0, available_column_names=input_operation_info.output_column_names
        )
        lineage_columns = set(required_input_columns)
        wanted_columns = lineage_columns
        if not global_state.keep_all_source_columns:
            wanted_columns = lineage_columns.difference(self._exclude_source_columns())
        parent_columns = self._get_parent_columns(input_operation_info.columns)
        agg_func = self._get_agg_func()
        if parent_columns:
            wanted_columns.update([
                parent_column.name for parent_column in parent_columns if parent_column.name
            ])

        other_node_names = set()
        columns = []
        for col in input_operation_info.columns:
            if col.name in lineage_columns:
                other_node_names.update(col.node_names)
            if col.name in wanted_columns:
                columns.append(col)

        columns = [col for col in input_operation_info.columns if col.name in wanted_columns]
        if not wanted_columns and agg_func == AggFunc.COUNT:
            # for groupby aggregation, the wanted columns is empty
            # in this case, take the first column from input operation info
            columns = input_operation_info.columns[:1]

        output_category = NodeOutputCategory.FEATURE
        if self.type in {
            NodeType.FORWARD_AGGREGATE,
            NodeType.LOOKUP_TARGET,
            NodeType.FORWARD_AGGREGATE_AS_AT,
        }:
            output_category = NodeOutputCategory.TARGET

        # prepare output variable type
        if agg_func:
            assert isinstance(self.parameters, BaseGroupbyParameters)
            aggregation_func_obj = construct_agg_func(agg_func)
            input_dtype_info = (
                parent_columns[0].dtype_info if parent_columns else columns[0].dtype_info
            )
            output_dtype_info = aggregation_func_obj.derive_output_dtype_info(
                input_dtype_info=input_dtype_info, category=self.parameters.value_by
            )
        else:
            # to be overridden at the _get_aggregations
            output_dtype_info = DBVarTypeInfo(dtype=DBVarType.UNKNOWN)

        return OperationStructure(
            columns=columns,
            aggregations=self._get_aggregations(  # type: ignore
                columns,
                node_name=self.name,
                other_node_names=other_node_names,
                output_dtype_info=output_dtype_info,
            ),
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=(self.name,),
            is_time_based=self._is_time_based(),
        )
