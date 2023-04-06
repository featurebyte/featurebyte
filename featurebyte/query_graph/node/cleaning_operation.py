"""
This module contains cleaning operation related classes.
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import TYPE_CHECKING, List, Literal, Sequence, Union
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

import pandas as pd
from pydantic import Field, validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.typing import Numeric, OptionalScalar
from featurebyte.enum import DBVarType, StrEnum
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.metadata.sdk_code import ClassEnum, ObjectClass
from featurebyte.query_graph.node.validator import construct_unique_name_validator

if TYPE_CHECKING:
    from featurebyte.query_graph.graph_node.base import GraphNode
    from featurebyte.query_graph.node import Node


class ConditionOperationField(StrEnum):
    """Field values used in critical data info operation"""

    MISSING = "missing"
    DISGUISED = "disguised"
    NOT_IN = "not_in"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    IS_STRING = "is_string"


class BaseCleaningOperation(FeatureByteBaseModel):
    """BaseCleaningOperation class"""

    imputed_value: OptionalScalar

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.dict()})"

    def add_cleaning_operation(
        self, graph_node: "GraphNode", input_node: "Node", dtype: DBVarType
    ) -> "Node":
        """
        Add cleaning operation to the graph node

        Parameters
        ----------
        graph_node: GraphNode
            Nested graph node
        input_node: Node
            Input node to the query graph
        dtype: DBVarType
            Data type that output column will be casted to

        Returns
        -------
        Node
        """
        condition_node = self.add_condition_operation(graph_node=graph_node, input_node=input_node)
        cond_assign_node = graph_node.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params={"value": self.imputed_value},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node, condition_node],
        )
        # explicitly cast the output back to original type if `dtype.to_type_str()` is not None
        type_str = DBVarType(dtype).to_type_str()
        if type_str:
            graph_node.add_operation(
                node_type=NodeType.CAST,
                node_params={"type": type_str, "from_dtype": dtype},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[cond_assign_node],
            )
        return graph_node.output_node

    @abstractmethod
    def derive_sdk_code(self) -> ObjectClass:
        """
        Derive SDK code for the current cleaning operation

        Returns
        -------
        ObjectClass
        """

    @abstractmethod
    def check_condition(self, value: OptionalScalar) -> bool:
        """
        Check whether the value fulfill this condition

        Parameters
        ----------
        value: OptionalScalar
            Value to be checked

        Returns
        -------
        bool
        """

    @abstractmethod
    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        """
        Construct a node that generate conditional filtering column

        Parameters
        ----------
        graph_node: GraphNode
            Graph node
        input_node: Node
            Input (series) node

        Returns
        -------
        Node
        """


class MissingValueImputation(BaseCleaningOperation):
    """
    MissingValueImputation class is used to impute the missing value of a table column.

    imputed_value: OptionalScalar
        Value to replace missing value

    Examples
    --------
    Create an imputation rule to replace missing value with 0

    >>> MissingValueImputation(imputed_value=0) # doctest: +SKIP
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.MissingValueImputation")

    type: Literal[ConditionOperationField.MISSING] = Field(
        ConditionOperationField.MISSING, const=True
    )

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.MISSING_VALUE_IMPUTATION(imputed_value=self.imputed_value)

    def check_condition(self, value: OptionalScalar) -> bool:
        return bool(pd.isnull(value))

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_NULL,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class DisguisedValueImputation(BaseCleaningOperation):
    """
    DisguisedValueImputation class is used to impute the disguised value of a table column.

    disguised_values: List[OptionalScalar]
        List of disguised values
    imputed_value: OptionalScalar
        Value to replace disguised value

    Examples
    --------
    Create an imputation rule to replace -999 with 0

    >>> DisguisedValueImputation(disguised_values=[-999], imputed_value=0) # doctest: +SKIP
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.DisguisedValueImputation")

    type: Literal[ConditionOperationField.DISGUISED] = Field(
        ConditionOperationField.DISGUISED, const=True
    )
    disguised_values: Sequence[OptionalScalar]

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.DISGUISED_VALUE_IMPUTATION(
            imputed_value=self.imputed_value, disguised_values=self.disguised_values
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        return value in self.disguised_values

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"value": self.disguised_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class UnexpectedValueImputation(BaseCleaningOperation):
    """
    UnexpectedValueImputation class is used to impute the unexpected value of a table column.
    Note that this imputation operation will not impute missing value.

    expected_values: List[OptionalScalar]
        List of expected values, values not in expected value will be imputed
    imputed_value: OptionalScalar
        Value to replace unexpected value

    Examples
    --------
    Create an imputation rule to replace value other than "buy" or "sell" to "missing"

    >>> UnexpectedValueImputation(expected_values=["buy", "sell"], imputed_value="missing") # doctest: +SKIP
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.UnexpectedValueImputation")

    type: Literal[ConditionOperationField.NOT_IN] = Field(
        ConditionOperationField.NOT_IN, const=True
    )
    expected_values: Sequence[OptionalScalar]

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.UNEXPECTED_VALUE_IMPUTATION(
            imputed_value=self.imputed_value, expected_values=self.expected_values
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        return value not in self.expected_values

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        isin_node = graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"value": self.expected_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )
        return graph_node.add_operation(
            node_type=NodeType.NOT,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[isin_node],
        )


class ValueBeyondEndpointImputation(BaseCleaningOperation):
    """
    ValueBeyondEndpointImputation class is used to impute the value exceed a specified endpoint

    type: Literal["less_than", "less_than_or_equal", "greater_than", "greater_than_or_equal"]
        Boundary type
    end_point: Union[int, float]
        End point
    imputed_value: OptionalScalar
        Value to replace value outside the end point boundary

    Examples
    --------
    Create an imputation rule to replace value less than 0 to 0

    >>> ValueBeyondEndpointImputation(type="less_than", end_point=0, imputed_value=0) # doctest: +SKIP
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.ValueBeyondEndpointImputation")

    type: Literal[
        ConditionOperationField.LESS_THAN,
        ConditionOperationField.LESS_THAN_OR_EQUAL,
        ConditionOperationField.GREATER_THAN,
        ConditionOperationField.GREATER_THAN_OR_EQUAL,
    ] = Field(allow_mutation=False)
    end_point: Numeric

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.VALUE_BEYOND_ENDPOINT_IMPUTATION(
            type=self.type, end_point=self.end_point, imputed_value=self.imputed_value
        )

    def check_condition(self, value: OptionalScalar) -> bool:
        operation_map = {
            ConditionOperationField.LESS_THAN: lambda x: x < self.end_point,
            ConditionOperationField.LESS_THAN_OR_EQUAL: lambda x: x <= self.end_point,
            ConditionOperationField.GREATER_THAN: lambda x: x > self.end_point,
            ConditionOperationField.GREATER_THAN_OR_EQUAL: lambda x: x >= self.end_point,
        }
        try:
            return bool(operation_map[self.type](value))
        except TypeError:
            # TypeError exception implies that two value are incomparable
            return False

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        node_type_map = {
            ConditionOperationField.LESS_THAN: NodeType.LT,
            ConditionOperationField.LESS_THAN_OR_EQUAL: NodeType.LE,
            ConditionOperationField.GREATER_THAN: NodeType.GT,
            ConditionOperationField.GREATER_THAN_OR_EQUAL: NodeType.GE,
        }
        return graph_node.add_operation(
            node_type=node_type_map[self.type],
            node_params={"value": self.end_point},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class StringValueImputation(BaseCleaningOperation):
    """
    StringValueImputation class is used to impute those value which is string type

    imputed_value: OptionalScalar
        Value to replace string value

    Examples
    --------
    Create an imputation rule to replace string value with 0

    >>> StringValueImputation(imputed_value=0) # doctest: +SKIP
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.StringValueImputation")

    type: Literal[ConditionOperationField.IS_STRING] = Field(
        ConditionOperationField.IS_STRING, const=True
    )

    def derive_sdk_code(self) -> ObjectClass:
        return ClassEnum.STRING_VALUE_IMPUTATION(imputed_value=self.imputed_value)

    def check_condition(self, value: OptionalScalar) -> bool:
        return isinstance(value, str)

    def add_condition_operation(self, graph_node: "GraphNode", input_node: "Node") -> "Node":
        return graph_node.add_operation(
            node_type=NodeType.IS_STRING,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


CleaningOperation = Annotated[
    Union[
        MissingValueImputation,
        DisguisedValueImputation,
        UnexpectedValueImputation,
        ValueBeyondEndpointImputation,
        StringValueImputation,
    ],
    Field(discriminator="type"),
]


class ColumnCleaningOperation(FeatureByteBaseModel):
    """
    ColumnCleaningOperation schema
    """

    column_name: str
    cleaning_operations: Sequence[CleaningOperation]


class TableCleaningOperation(FeatureByteBaseModel):
    """
    TableCleaningOperation schema
    """

    table_name: str
    column_cleaning_operations: List[ColumnCleaningOperation]

    # pydantic validators
    _validate_unique_column_name = validator("column_cleaning_operations", allow_reuse=True)(
        construct_unique_name_validator(field="column_name")
    )
