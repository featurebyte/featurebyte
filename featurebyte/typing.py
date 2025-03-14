"""
Common utilities related to typing
"""

from typing import Any, Callable, Optional, Sequence, Type, Union, cast

import pandas as pd
from pandas.api.types import is_scalar as pd_is_scalar
from pydantic import StrictFloat, StrictInt, StrictStr
from typing_extensions import Literal

DatetimeSupportedPropertyType = Literal[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "dayofweek",
    "hour",
    "minute",
    "second",
]
TimedeltaSupportedUnitType = Literal[
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
]

Scalar = Union[bool, StrictInt, StrictFloat, StrictStr]
OptionalScalar = Optional[Scalar]
ScalarSequence = Sequence[Scalar]
Numeric = Union[StrictInt, StrictFloat]
Timestamp = Union[pd.Timestamp]

AllSupportedValueTypes = Union[Scalar, ScalarSequence, Timestamp]

Func = Callable[..., Any]


class Unset:
    """
    Placeholder class for unset values
    """

    def __repr__(self) -> str:
        return "UNSET"

    def __str__(self) -> str:
        return "UNSET"


UNSET = Unset()


def is_scalar(value: Any) -> bool:
    """
    Returns whether the provided value is a Scalar value

    Parameters
    ----------
    value : Any
        Value to check

    Returns
    -------
    bool
    """
    return cast(bool, pd_is_scalar(value))


def is_scalar_nan(value: Any) -> bool:
    """
    Returns whether the provided value is a scalar nan value (float('nan'), np.nan, None)

    Parameters
    ----------
    value : Any
        Value to check

    Returns
    -------
    bool
    """
    if not is_scalar(value):
        return False
    return cast(bool, pd.isna(value))


def get_or_default(value: Optional[Any], default_value: Any) -> Any:
    """
    Returns the default value if the value passed in is None.

    Parameters
    ----------
    value: Optional[Any]
        value to check if optional
    default_value: Any
        default value to return

    Returns
    -------
    Any
        value to use
    """
    if value is not None:
        return value
    return default_value


def validate_type_is(obj: Any, obj_name: str, expected_type: Type[Any]) -> None:
    """
    Check that obj is of type expected_type. If not, raise a TypeError.

    Parameters
    ----------
    obj: Any
        Object to check
    obj_name: str
        Name of the object to check
    expected_type: Type[Any]
        Expected type of obj

    Raises
    ------
    TypeError
        If obj is not of type expected_type
    """
    if not isinstance(obj, expected_type):
        msg = f'type of argument "{obj_name}" must be {expected_type.__name__}; got {type(obj).__name__} instead'
        raise TypeError(msg)


def validate_type_is_feature(obj: Any, obj_name: str) -> None:
    """
    Check that obj is of type expected_type. If not, raise a TypeError.

    Parameters
    ----------
    obj: Any
        Object to check
    obj_name: str
        Name of the object to check
    """

    from featurebyte.api.feature import Feature

    validate_type_is(obj, obj_name, Feature)
