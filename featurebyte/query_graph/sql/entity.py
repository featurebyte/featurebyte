"""
Entity related helpers
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier

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


def get_combined_serving_names_expr(
    serving_names: Sequence[Union[str, Expression]],
    serving_names_table_alias: Optional[str] = None,
) -> Expression:
    """
    Get an expression for the concatenated serving names

    Parameters
    ----------
    serving_names: List[str]
        Column names of the serving names to be concatenated
    serving_names_table_alias: Optional[str]
        Table alias for the serving names. Serving names will not be table qualified if not
        provided.

    Returns
    -------
    Expression
    """
    assert len(serving_names) > 0
    parts: List[Expression] = []
    for serving_name in serving_names:
        if isinstance(serving_name, str):
            if serving_names_table_alias is not None:
                serving_name_expr = get_qualified_column_identifier(
                    serving_name, serving_names_table_alias
                )
            else:
                serving_name_expr = quoted_identifier(serving_name)
        else:
            serving_name_expr = serving_name
        expr = expressions.Cast(this=serving_name_expr, to=expressions.DataType.build("VARCHAR"))
        parts.append(expr)
        parts.append(make_literal_value("::"))
    combined_serving_names_expr = expressions.Coalesce(
        this=expressions.Concat(expressions=parts[:-1]),
        expressions=[make_literal_value("")],
    )
    return combined_serving_names_expr


def get_combined_serving_names_python(serving_name_cols: List[Any]) -> str:
    """
    Construct concatenated serving names for a row

    This must apply the exact same transformations as get_combined_serving_names_expr()

    Parameters
    ----------
    serving_name_cols: List[Any]
        List of the serving names to be concatenated for a row

    Returns
    -------
    str
    """
    assert len(serving_name_cols) > 0
    serving_name_cols = [str(col) for col in serving_name_cols]
    return "::".join(serving_name_cols)
