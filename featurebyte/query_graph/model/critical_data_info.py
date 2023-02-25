"""
This module contains critical data info related models.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Literal, Sequence, Union
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from abc import abstractmethod

import pandas as pd
from pydantic import Field, validator

from featurebyte.common.typing import OptionalScalar
from featurebyte.enum import DBVarType, StrEnum
from featurebyte.exception import InvalidImputationsError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import BaseCleaningOperation


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


NumericT = Union[int, float]
IMPUTE_OPERATIONS = []


class BaseImputeOperation(BaseCleaningOperation):
    """BaseImputeOperation class"""

    imputed_value: OptionalScalar

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add impute class to IMPUTE_OPERATIONS
            IMPUTE_OPERATIONS.append(cls)

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.dict()})"

    def add_cleaning_operation(
        self, graph_node: GraphNode, input_node: Node, dtype: DBVarType
    ) -> Node:
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
    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
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


class BaseCondition(FeatureByteBaseModel):
    """Base condition model"""

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
    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
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


class MissingValueCondition(BaseCondition):
    """Missing value condition"""

    type: Literal[ConditionOperationField.MISSING] = Field(
        ConditionOperationField.MISSING, const=True
    )

    def check_condition(self, value: OptionalScalar) -> bool:
        return bool(pd.isnull(value))

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        return graph_node.add_operation(
            node_type=NodeType.IS_NULL,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class DisguisedValueCondition(BaseCondition):
    """Disguised value condition"""

    type: Literal[ConditionOperationField.DISGUISED] = Field(
        ConditionOperationField.DISGUISED, const=True
    )
    disguised_values: Sequence[OptionalScalar]

    def check_condition(self, value: OptionalScalar) -> bool:
        return value in self.disguised_values

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        return graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"value": self.disguised_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class UnexpectedValueCondition(BaseCondition):
    """Unexpected value condition"""

    type: Literal[ConditionOperationField.NOT_IN] = Field(
        ConditionOperationField.NOT_IN, const=True
    )
    expected_values: Sequence[OptionalScalar]

    def check_condition(self, value: OptionalScalar) -> bool:
        return value not in self.expected_values

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
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


class BoundaryCondition(BaseCondition):
    """Boundary value condition"""

    type: Literal[
        ConditionOperationField.LESS_THAN,
        ConditionOperationField.LESS_THAN_OR_EQUAL,
        ConditionOperationField.GREATER_THAN,
        ConditionOperationField.GREATER_THAN_OR_EQUAL,
    ] = Field(allow_mutation=False)
    end_point: NumericT

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

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
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


class IsStringCondition(BaseCondition):
    """Is string condition"""

    type: Literal[ConditionOperationField.IS_STRING] = Field(
        ConditionOperationField.IS_STRING, const=True
    )

    def check_condition(self, value: OptionalScalar) -> bool:
        return isinstance(value, str)

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        return graph_node.add_operation(
            node_type=NodeType.IS_STRING,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class MissingValueImputation(MissingValueCondition, BaseImputeOperation):
    """
    MissingValueImputation class is used to impute the missing value of a data column.

    imputed_value: OptionalScalar
        Value to replace missing value

    Examples
    --------
    Create an imputation rule to replace missing value with 0

    >>> MissingValueImputation(imputed_value=0) # doctest: +SKIP
    """


class DisguisedValueImputation(DisguisedValueCondition, BaseImputeOperation):
    """
    DisguisedValueImputation class is used to impute the disguised missing value of a data column.

    disguised_values: List[OptionalScalar]
        List of disguised missing values
    imputed_value: OptionalScalar
        Value to replace disguised missing value

    Examples
    --------
    Create an imputation rule to replace -999 with 0

    >>> DisguisedValueImputation(disguised_values=[-999], imputed_value=0) # doctest: +SKIP
    """


class UnexpectedValueImputation(UnexpectedValueCondition, BaseImputeOperation):
    """
    UnexpectedValueImputation class is used to impute the unexpected value of a data column.
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


class ValueBeyondEndpointImputation(BoundaryCondition, BaseImputeOperation):
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


class StringValueImputation(IsStringCondition, BaseImputeOperation):
    """
    StringValueImputation class is used to impute those value which is string type

    imputed_value: OptionalScalar
        Value to replace string value

    Examples
    --------
    Create an imputation rule to replace string value with 0

    >>> StringValueImputation(imputed_value=0) # doctest: +SKIP
    """


if TYPE_CHECKING:
    # use BaseCleaning for type checking
    CleaningOperation = BaseCleaningOperation
else:
    # use Annotated types for type checking during runtime
    CleaningOperation = Annotated[Union[tuple(IMPUTE_OPERATIONS)], Field(discriminator="type")]


class CriticalDataInfo(FeatureByteBaseModel):
    """Critical data info model"""

    cleaning_operations: List[CleaningOperation]

    @validator("cleaning_operations")
    @classmethod
    def _validate_cleaning_operation(
        cls, values: List[CleaningOperation]
    ) -> List[CleaningOperation]:
        error_message = (
            "Column values imputed by {first_imputation} will be imputed by {second_imputation}. "
            "Please revise the imputations so that no value could be imputed twice."
        )
        # check that there is no double imputations in the list of impute operations
        # for example, if -999 is imputed to None, then we impute None to 0.
        # in this case, -999 value is imputed twice (first it is imputed to None, then is imputed to 0).
        imputations = [op for op in values if isinstance(op, BaseImputeOperation)]
        for i, imputation in enumerate(imputations[:-1]):
            for other_imputation in imputations[(i + 1) :]:
                if other_imputation.check_condition(imputation.imputed_value):  # type: ignore
                    raise InvalidImputationsError(
                        error_message.format(
                            first_imputation=imputation, second_imputation=other_imputation
                        )
                    )
        return values
