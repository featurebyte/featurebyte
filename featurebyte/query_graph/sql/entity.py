"""
Entity related helpers
"""
from __future__ import annotations

from typing import List

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier

DUMMY_ENTITY_COLUMN_NAME = "__featurebyte_dummy_entity"
DUMMY_ENTITY_VALUE = "0"


def get_combined_serving_names(serving_names: List[str]) -> str:
    """
    Get column name of the combined serving names

    Parameters
    ----------
    serving_names: List[str]
        Column names of the serving names to be concatenated

    Returns
    -------
    str
    """
    combined_serving_names = " x ".join(serving_names)
    return combined_serving_names


def get_combined_serving_names_expr(serving_names: List[str]) -> Expression:
    """
    Get an expression for the concatenated serving names

    Parameters
    ----------
    serving_names: List[str]
        Column names of the serving names to be concatenated

    Returns
    -------
    Expression
    """
    assert len(serving_names) > 0
    parts: List[Expression] = []
    for serving_name in serving_names:
        expr = expressions.Cast(
            this=quoted_identifier(serving_name), to=expressions.DataType.build("VARCHAR")
        )
        parts.append(expr)
        parts.append(make_literal_value("::"))
    combined_serving_names_expr = expressions.Coalesce(
        this=expressions.Concat(expressions=parts[:-1]),
        expressions=[make_literal_value("")],
    )
    return combined_serving_names_expr


def get_combined_serving_names_pandas(serving_name_cols: List[pd.Series]) -> pd.Series:
    """
    Construct a pandas series of the concatenated serving names

    This must apply the exact same transformations as get_combined_serving_names_expr()

    Parameters
    ----------
    serving_name_cols: List[pd.Series]
        Pandas series of the serving names to be concatenated

    Returns
    -------
    pd.Series
    """
    assert len(serving_name_cols) > 0
    serving_name_cols = [col.astype(str) for col in serving_name_cols]
    result = serving_name_cols[0]
    for col in serving_name_cols[1:]:
        result = result + "::" + col
    result = result.fillna("")
    return result
