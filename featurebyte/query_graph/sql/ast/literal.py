"""
Module for literal value handling
"""
from __future__ import annotations

from typing import Any, cast

from sqlglot import expressions, parse_one

from featurebyte.common.typing import is_scalar_nan


def make_literal_value(value: Any, cast_as_timestamp: bool = False) -> expressions.Expression:
    """Create a sqlglot literal value

    Parameters
    ----------
    value : ValueParameterType
        The literal value
    cast_as_timestamp : bool
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
    if cast_as_timestamp:
        return cast(expressions.Expression, parse_one(f"CAST('{str(value)}' AS TIMESTAMP)"))
    if isinstance(value, str):
        return expressions.Literal.string(value)
    if isinstance(value, bool):
        return expressions.Boolean(this=value)
    if isinstance(value, dict):
        assert value["type"] == "timestamp"
        return expressions.Anonymous(
            this="TO_TIMESTAMP", expressions=[expressions.Literal.string(value["iso_format_str"])]
        )
    return expressions.Literal.number(value)
