"""
Module for literal value handling
"""

from __future__ import annotations

from typing import Any, cast

from sqlglot import expressions, parse_one

from featurebyte.query_graph.node.scalar import NonNativeValueType, TimestampValue
from featurebyte.typing import is_scalar_nan


def make_literal_value_from_non_native_types(value: dict[str, Any]) -> expressions.Expression:
    """Create a sqlglot literal value from non-native types

    Parameters
    ----------
    value: dict[str, Any]
        The literal value

    Returns
    -------
    Expression
    """
    assert value["type"] == NonNativeValueType.TIMESTAMP
    timestamp_value = TimestampValue(**value)
    return expressions.Anonymous(
        this="TO_TIMESTAMP",
        expressions=[expressions.Literal.string(timestamp_value.get_isoformat_utc())],
    )


def make_literal_value_from_native_types(
    value: Any, cast_as_timestamp: bool = False
) -> expressions.Expression:
    """
    Create a sqlglot literal value from native types

    Parameters
    ----------
    value: Any
        The literal value
    cast_as_timestamp : bool
        Whether to cast the value to timestamp

    Returns
    -------
    Expression
    """
    if cast_as_timestamp:
        return cast(expressions.Expression, parse_one(f"CAST('{str(value)}' AS TIMESTAMP)"))
    if isinstance(value, str):
        return expressions.Literal.string(value)
    if isinstance(value, bool):
        return expressions.Boolean(this=value)
    return expressions.Literal.number(value)


def make_literal_value(value: Any, cast_as_timestamp: bool = False) -> expressions.Expression:
    """Create a sqlglot literal value

    Parameters
    ----------
    value: Any
        The literal value
    cast_as_timestamp: bool
        Whether to cast the value to timestamp

    Returns
    -------
    Expression
    """
    if is_scalar_nan(value):
        return expressions.Null()
    if isinstance(value, list):
        return expressions.Array(
            expressions=[make_literal_value(v, cast_as_timestamp=cast_as_timestamp) for v in value]
        )
    if isinstance(value, dict):
        return make_literal_value_from_non_native_types(value)
    return make_literal_value_from_native_types(value, cast_as_timestamp=cast_as_timestamp)
