"""
Base classes required for constructing query graph nodes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Dict, List, Optional, Sequence, Type, TypeVar, Union

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import (
    DerivedDataColumn,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
    PostAggregationColumn,
)

NODE_TYPES = []
NodeT = TypeVar("NodeT", bound="BaseNode")


class BaseNode(BaseModel):
    """
    BaseNode class
    """

    name: str
    type: NodeType
    output_type: NodeOutputType
    parameters: BaseModel

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # make sure subclass set certain properties correctly
        assert self.__fields__["type"].field_info.const is True
        assert repr(self.__fields__["type"].type_).startswith("typing.Literal")
        assert self.__fields__["output_type"].type_ is NodeOutputType

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add node type class to NODE_TYPES if the type variable is a literal (to filter out base classes)
            NODE_TYPES.append(cls)

    @property
    def transform_info(self) -> str:
        """
        Construct from node transform object from this node

        Returns
        -------
        str
        """
        parameters = sorted(
            f"{key}='{value}'" if isinstance(value, str) else f"{key}={value}"
            for key, value in self.parameters.dict().items()
            if value
        )
        if parameters and len(parameters) < 4:
            # Note: 4 is chosen here so that the info is more readable, with too many
            # parameters presented here, it is hard to read. This value currently is only
            # used for the signal type tagging (for feature theme).
            return f"{str(self.type).lower()}({', '.join(parameters)})"
        return str(self.type).lower()

    @classmethod
    def detect_var_type_from_value(cls, value: Any) -> DBVarType:
        """
        Detect variable type of the given scalar value

        Parameters
        ----------
        value: Any
            Input value

        Returns
        -------
        DBVarType
        """
        if isinstance(value, bool):
            return DBVarType.BOOL
        if isinstance(value, int):
            return DBVarType.INT
        if isinstance(value, float):
            return DBVarType.FLOAT
        if isinstance(value, str):
            return DBVarType.VARCHAR
        return DBVarType.UNKNOWN

    @classmethod
    def _extract_column_str_values(
        cls,
        values: Any,
        column_str_type: Union[Type[InColumnStr], Type[OutColumnStr]],
    ) -> List[str]:
        out = set()
        if isinstance(values, dict):
            for val in values.values():
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        if isinstance(values, list):
            for val in values:
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        return list(out)

    def get_required_input_columns(self) -> List[str]:
        """
        Get the required input column names based on this node parameters

        Returns
        -------
        list[str]
        """
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def get_new_output_columns(self) -> List[str]:
        """
        Get additional column names generated based on this node parameters

        Returns
        -------
        list[str]
        """
        return self._extract_column_str_values(self.parameters.dict(), OutColumnStr)

    def derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        operation_info = self._derive_node_operation_info(
            inputs=inputs, branch_state=branch_state, global_state=global_state
        )
        if operation_info.columns or operation_info.aggregations:
            # make sure node name should be included in the node operation info
            assert self.name in operation_info.all_node_names
        # Update is_time_based based on the inputs, or if the derive_node_operation_info returns true
        update_args = {
            "is_time_based": any(input_.is_time_based for input_ in inputs)
            or operation_info.is_time_based,
        }
        return OperationStructure(**{**operation_info.dict(), **update_args})

    def clone(self: NodeT, **kwargs: Any) -> NodeT:
        """
        Clone an existing object with certain update

        Parameters
        ----------
        kwargs: Any
            Keyword parameters to overwrite existing object

        Returns
        -------
        NodeT
        """
        return type(self)(**{**self.dict(), **kwargs})

    def prune(
        self: NodeT,
        target_nodes: Sequence[NodeT],
        input_operation_structures: List[OperationStructure],
    ) -> NodeT:
        """
        Prune this node parameters based on target nodes

        Parameters
        ----------
        target_nodes: Sequence[BaseNode]
            List of target nodes
        input_operation_structures: List[OperationStructure]
            List of input operation structures

        Returns
        -------
        NodeT
        """
        _ = target_nodes, input_operation_structures
        return self

    @abstractmethod
    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info abstract method to be implemented at the concrete node class

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """


class SeriesOutputNodeOpStructMixin:
    """SeriesOutputNodeOpStructMixin class"""

    name: str
    transform_info: str
    output_type: NodeOutputType

    @abstractmethod
    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        """
        Derive variable type from the input operation structures

        Parameters
        ----------
        inputs: List[OperationStructure]
            Operation structures of the input nodes

        Returns
        -------
        DBVarType
        """

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        branch_state: OperationStructureBranchState
            State captures the graph branching state info
        global_state: OperationStructureInfo
            State captures the global graph info (used during operation structure derivation)

        Returns
        -------
        OperationStructure
        """
        _ = branch_state, global_state
        input_operation_info = inputs[0]
        output_category = input_operation_info.output_category
        columns = []
        aggregations = []
        for inp in inputs:
            columns.extend(inp.columns)
            aggregations.extend(inp.aggregations)

        node_kwargs: Dict[str, Any] = {}
        if output_category == NodeOutputCategory.VIEW:
            node_kwargs["columns"] = [
                DerivedDataColumn.create(
                    name=None,
                    columns=columns,
                    transform=self.transform_info,
                    node_name=self.name,
                    dtype=self.derive_var_type(inputs),
                )
            ]
        else:
            node_kwargs["columns"] = columns
            node_kwargs["aggregations"] = [
                PostAggregationColumn.create(
                    name=None,
                    columns=aggregations,
                    transform=self.transform_info,
                    node_name=self.name,
                    dtype=self.derive_var_type(inputs),
                )
            ]

        return OperationStructure(
            **node_kwargs,
            output_type=self.output_type,
            output_category=output_category,
            row_index_lineage=input_operation_info.row_index_lineage,
        )


class BaseSeriesOutputNode(SeriesOutputNodeOpStructMixin, BaseNode, ABC):
    """Base class for node produces series output"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)


class BaseSeriesOutputWithAScalarParamNode(SeriesOutputNodeOpStructMixin, BaseNode, ABC):
    """Base class for node produces series output & contain a single scalar parameter"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Any]

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters


class BinaryLogicalOpMixin(BaseSeriesOutputWithAScalarParamNode):
    """BinaryLogicalOpMixin class"""

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class BinaryRelationalOpMixin(BaseSeriesOutputWithAScalarParamNode):
    """BinaryRelationalOpMixin class"""

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class BinaryArithmeticOpMixin(SeriesOutputNodeOpStructMixin):
    """BinaryArithmeticOpMixin class"""

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        input_var_types = {inp.series_output_dtype for inp in inputs}
        if DBVarType.FLOAT in input_var_types:
            return DBVarType.FLOAT
        return inputs[0].series_output_dtype


class BasePrunableNode(BaseNode):
    """Base class for node that can be pruned during query graph pruning"""

    @abstractmethod
    def resolve_node_pruned(self, input_node_names: List[str]) -> str:
        """
        Method used to resolve the situation when the node get pruned. As all the nodes only produce single
        output, we should only choose one node from the input nodes.

        Parameters
        ----------
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        str
            Node name selected to replace this (pruned) node
        """
