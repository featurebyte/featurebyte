"""
Module for literal value handling
"""
from __future__ import annotations

from typing import Any

from sqlglot import expressions, parse_one

from featurebyte.common.typing import is_scalar_nan


def make_literal_value(value: Any, cast_as_timestamp: bool = False) -> expressions.Expression:
    """Create a sqlglot literal value

    Parameters
    ----------
    value : Any
        The literal value
    cast_as_timestamp : bool
        Whether to cast the value to timestamp

    Returns
    -------
    Expression
    """
    if is_scalar_nan(value):
        return expressions.Null()
    if cast_as_timestamp:
        return parse_one(f"CAST('{str(value)}' AS TIMESTAMP)")
    if isinstance(value, str):
        return expressions.Literal.string(value)
    return expressions.Literal.number(value)
