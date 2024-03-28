"""
This module contains critical data info related models.
"""

from __future__ import annotations

from typing import List

from pydantic import validator

from featurebyte.exception import InvalidImputationsError
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.node.cleaning_operation import BaseCleaningOperation, CleaningOperation


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
        imputations = [op for op in values if isinstance(op, BaseCleaningOperation)]
        for i, imputation in enumerate(imputations[:-1]):
            for other_imputation in imputations[(i + 1) :]:
                if other_imputation.check_condition(imputation.imputed_value):
                    raise InvalidImputationsError(
                        error_message.format(
                            first_imputation=imputation, second_imputation=other_imputation
                        )
                    )
        return values
