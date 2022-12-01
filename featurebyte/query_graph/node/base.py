"""
Base classes required for constructing query graph nodes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, List, Optional, Set, Type, Union

from abc import abstractmethod

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.column import InColumnStr, OutColumnStr
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.mixin import SeriesOutputNodeOpStructMixin

NODE_TYPES = []


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

    @abstractmethod
    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        visited_node_types: Set[NodeType]
            Set of visited nodes when doing backward traversal

        Returns
        -------
        OperationStructure
        """


class BaseSeriesOutputNode(SeriesOutputNodeOpStructMixin, BaseNode):
    """Base class for node produces series output"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)


class BaseSeriesOutputWithAScalarParamNode(SeriesOutputNodeOpStructMixin, BaseNode):
    """Base class for node produces series output & contain a single scalar parameter"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Any]

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: Parameters
