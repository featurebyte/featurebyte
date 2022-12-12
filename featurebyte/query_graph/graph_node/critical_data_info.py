"""
This module contains critical data info related models.
"""
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Union
from typing_extensions import Annotated  # pylint: disable=wrong-import-order

from abc import abstractmethod

import pandas as pd
from pydantic import Field, validator

from featurebyte.enum import StrEnum
from featurebyte.exception import InvalidImputationsError
from featurebyte.models.base import FeatureByteBaseModel


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


IMPUTE_OPERATIONS = []
FLAG_OPERATIONS = []


class BaseImputeOperation(FeatureByteBaseModel):
    """BaseImputeOperation class"""

    imputed_value: Optional[Any]

    def __init_subclass__(cls, **kwargs: Any):
        if repr(cls.__fields__["type"].type_).startswith("typing.Literal"):
            # only add impute class to IMPUTE_OPERATIONS
            IMPUTE_OPERATIONS.append(cls)

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        return f"{class_name}({self.dict()})"


class BaseCondition(FeatureByteBaseModel):
    """Base condition model"""

    @abstractmethod
    def check_condition(self, value: Any) -> bool:
        """
        Check whether the value fulfill this condition

        Parameters
        ----------
        value: Any
            Value to be checked

        Returns
        -------
        bool
        """


class MissingValueCondition(BaseCondition):
    """Missing value condition"""

    type: Literal[ConditionOperationField.MISSING] = Field(
        ConditionOperationField.MISSING, const=True
    )

    def check_condition(self, value: Any) -> bool:
        return pd.isnull(value)


class DisguisedValueCondition(BaseCondition):
    """Missing value condition"""

    type: Literal[ConditionOperationField.DISGUISED] = Field(
        ConditionOperationField.DISGUISED, const=True
    )
    disguised_values: List[Any]

    def check_condition(self, value: Any) -> bool:
        return value in self.disguised_values


class UnexpectedValueCondition(BaseCondition):
    """Unexpected value condition"""

    type: Literal[ConditionOperationField.NOT_IN] = Field(
        ConditionOperationField.NOT_IN, const=True
    )
    expected_values: List[Any]

    def check_condition(self, value: Any) -> bool:
        return value not in self.expected_values


class BoundaryCondition(BaseCondition):
    """Boundary value condition"""

    type: Literal[
        ConditionOperationField.LESS_THAN,
        ConditionOperationField.LESS_THAN_OR_EQUAL,
        ConditionOperationField.GREATER_THAN,
        ConditionOperationField.GREATER_THAN_OR_EQUAL,
    ] = Field(allow_mutation=False)
    end_point: Any

    def check_condition(self, value: Any) -> bool:
        operation_map = {
            ConditionOperationField.LESS_THAN: lambda x: x < self.end_point,
            ConditionOperationField.LESS_THAN_OR_EQUAL: lambda x: x <= self.end_point,
            ConditionOperationField.GREATER_THAN: lambda x: x > self.end_point,
            ConditionOperationField.GREATER_THAN_OR_EQUAL: lambda x: x >= self.end_point,
        }
        try:
            return operation_map[self.type](value)
        except TypeError:
            # TypeError exception implies that two value are incomparable
            return False


class IsStringCondition(BaseCondition):
    """Is string condition"""

    type: Literal[ConditionOperationField.IS_STRING] = Field(
        ConditionOperationField.IS_STRING, const=True
    )

    def check_condition(self, value: Any) -> bool:
        return isinstance(value, str)


class MissingValueImputation(BaseImputeOperation, MissingValueCondition):
    """Impute missing value"""


class DisguisedValueImputation(BaseImputeOperation, DisguisedValueCondition):
    """Impute disguised values"""


class UnexpectedValueImputation(BaseImputeOperation, UnexpectedValueCondition):
    """Impute unexpected values"""


class ValueBeyondEndpointImputation(BaseImputeOperation, BoundaryCondition):
    """Impute values by specifying boundary"""


class StringValueImputation(BaseImputeOperation, IsStringCondition):
    """Impute is string value"""


if TYPE_CHECKING:
    # use BaseImputeOperation & BaseFlagOperation for type checking
    ImputeOperation = BaseImputeOperation
else:
    # use Annotated types for type checking during runtime
    ImputeOperation = Annotated[Union[tuple(IMPUTE_OPERATIONS)], Field(discriminator="type")]


class CriticalDataInfo(FeatureByteBaseModel):
    """Critical data info model"""

    imputations: List[ImputeOperation]

    @validator("imputations")
    @classmethod
    def _validate_imputations(cls, values: List[ImputeOperation]) -> List[ImputeOperation]:
        error_message = (
            "Column values imputed by {first_imputation} will be imputed by {second_imputation}. "
            "Please revise the imputations so that no value could be imputed twice."
        )
        # check that there is no double imputations in the list of impute operations
        for i, imputation in enumerate(values[:-1]):
            for other_imputation in values[(i + 1) :]:
                if other_imputation.check_condition(imputation.imputed_value):  # type: ignore
                    raise InvalidImputationsError(
                        error_message.format(
                            first_imputation=imputation, second_imputation=other_imputation
                        )
                    )
        return values
