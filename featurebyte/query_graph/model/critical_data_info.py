"""
This module contains critical data info related models.
"""

from __future__ import annotations

from typing import List

from pydantic import field_validator

from featurebyte.exception import CleaningOperationError, InvalidImputationsError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.node.cleaning_operation import (
    BaseImputationCleaningOperation,
    CleaningOperation,
    CleaningOperationType,
)


class CriticalDataInfo(FeatureByteBaseModel):
    """Critical data info model"""

    cleaning_operations: List[CleaningOperation]

    @field_validator("cleaning_operations")
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
        imputations = [op for op in values if isinstance(op, BaseImputationCleaningOperation)]
        for i, imputation in enumerate(imputations[:-1]):
            for other_imputation in imputations[(i + 1) :]:
                if other_imputation.check_condition(imputation.imputed_value):
                    raise InvalidImputationsError(
                        error_message.format(
                            first_imputation=imputation, second_imputation=other_imputation
                        )
                    )

        # check whether add timestamp cleaning operation is valid
        # if there is a timestamp cleaning operation, it should be the last operation
        ts_schema_cleaning_ops = [
            op for op in values if op.type == CleaningOperationType.ADD_TIMESTAMP_SCHEMA
        ]
        if ts_schema_cleaning_ops:
            if ts_schema_cleaning_ops[0] != values[-1]:
                raise CleaningOperationError("AddTimestampSchema must be the last operation.")

        return values
