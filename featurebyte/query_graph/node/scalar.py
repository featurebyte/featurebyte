"""
Models for the value parameter used by SingleValueNodeParameters
"""

from __future__ import annotations

from typing import Union, cast

import pandas as pd
from typing_extensions import Literal

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.typing import AllSupportedValueTypes, Scalar, ScalarSequence


class NonNativeValueType(StrEnum):
    """
    Scalar value types enum
    """

    TIMESTAMP = "timestamp"


class TimestampValue(FeatureByteBaseModel):
    """TimestampValue class"""

    iso_format_str: str
    type: Literal[NonNativeValueType.TIMESTAMP] = NonNativeValueType.TIMESTAMP

    @classmethod
    def from_pandas_timestamp(cls, timestamp: pd.Timestamp) -> TimestampValue:
        """
        Create a TimestampValue from a pandas timestamp.

        Parameters
        ----------
        timestamp: pd.Timestamp
            The pandas timestamp

        Returns
        -------
        TimestampValue
        """
        return TimestampValue(iso_format_str=timestamp.isoformat())

    def get_isoformat_utc(self) -> str:
        """
        Get the isoformat string in UTC but with timezone removed, if any.

        Returns
        -------
        str
        """
        timestamp = pd.Timestamp(self.iso_format_str)
        if timestamp.tz is not None:
            timestamp = timestamp.tz_convert("UTC").tz_localize(None)
        return cast(str, timestamp.isoformat())


ValueParameterType = Union[Scalar, ScalarSequence, TimestampValue]


def get_value_parameter(sdk_provided_value: AllSupportedValueTypes) -> ValueParameterType:
    """
    Returns the value parameter to be used in the query graph.

    Parameters
    ----------
    sdk_provided_value : AllSupportedValueTypes
        Value provided by user through the SDK

    Returns
    -------
    ValueParameterType
        Value parameter to be used in the query graph
    """
    if isinstance(sdk_provided_value, pd.Timestamp):
        return TimestampValue.from_pandas_timestamp(sdk_provided_value)
    return sdk_provided_value
