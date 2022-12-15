"""
This module contains critical data info related models.
"""
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Sequence, Union
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from abc import abstractmethod

import pandas as pd
from pydantic import Field, validator

from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidImputationsError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
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


ScalarT = Optional[Union[int, float, str, bool]]
NumericT = Union[int, float]
IMPUTE_OPERATIONS = []


class BaseCleaningOperation(FeatureByteBaseModel):
    """BaseCleaningOperation class"""


class BaseImputeOperation(BaseCleaningOperation):
    """BaseImputeOperation class"""

    imputed_value: ScalarT

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add impute class to IMPUTE_OPERATIONS
            IMPUTE_OPERATIONS.append(cls)

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.dict()})"

    def add_impute_operation(
        self, graph_node: GraphNode, input_node: Node, condition_node: Node
    ) -> "Node":
        """
        Add impute operation to the graph node

        Parameters
        ----------
        graph_node: GraphNode
            Nested graph node
        input_node: Node
            Input node to the query graph
        condition_node: Node
            Conditional node to the graph.

        Returns
        -------
        Node
        """
        graph_node.add_operation(
            node_type=NodeType.CONDITIONAL,
            node_params={"value": self.imputed_value},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node, condition_node],
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
    def check_condition(self, value: ScalarT) -> bool:
        """
        Check whether the value fulfill this condition

        Parameters
        ----------
        value: ScalarT
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

    def check_condition(self, value: ScalarT) -> bool:
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
    disguised_values: Sequence[ScalarT]

    def check_condition(self, value: ScalarT) -> bool:
        return value in self.disguised_values

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        return graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"values": self.disguised_values},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class UnexpectedValueCondition(BaseCondition):
    """Unexpected value condition"""

    type: Literal[ConditionOperationField.NOT_IN] = Field(
        ConditionOperationField.NOT_IN, const=True
    )
    expected_values: Sequence[ScalarT]

    def check_condition(self, value: ScalarT) -> bool:
        return value not in self.expected_values

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        isin_node = graph_node.add_operation(
            node_type=NodeType.IS_IN,
            node_params={"values": self.expected_values},
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

    def check_condition(self, value: ScalarT) -> bool:
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

    def check_condition(self, value: ScalarT) -> bool:
        return isinstance(value, str)

    def add_condition_operation(self, graph_node: GraphNode, input_node: Node) -> Node:
        return graph_node.add_operation(
            node_type=NodeType.IS_STRING,
            node_params={},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[input_node],
        )


class MissingValueImputation(MissingValueCondition, BaseImputeOperation):
    """Impute missing value"""


class DisguisedValueImputation(DisguisedValueCondition, BaseImputeOperation):
    """Impute disguised values"""


class UnexpectedValueImputation(UnexpectedValueCondition, BaseImputeOperation):
    """Impute unexpected values"""


class ValueBeyondEndpointImputation(BoundaryCondition, BaseImputeOperation):
    """Impute values by specifying boundary"""


class StringValueImputation(IsStringCondition, BaseImputeOperation):
    """Impute is string value"""


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
        for i, imputation in enumerate(values[:-1]):
            for other_imputation in values[(i + 1) :]:
                if other_imputation.check_condition(imputation.imputed_value):  # type: ignore
                    raise InvalidImputationsError(
                        error_message.format(
                            first_imputation=imputation, second_imputation=other_imputation
                        )
                    )
        return values
