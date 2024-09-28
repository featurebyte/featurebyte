"""
Aggregate asat helper functions
"""

from __future__ import annotations

from typing import Optional, Tuple

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.scd_helper import END_TS, augment_scd_table_with_end_timestamp


def ensure_end_timestamp_column(
    effective_timestamp_column: str,
    natural_key_column: Optional[str],
    end_timestamp_column: Optional[str],
    source_expr: Select,
) -> Tuple[str, Select]:
    """
    Ensure the end timestamp column is present in the source expression for SCD table

    Parameters
    ----------
    effective_timestamp_column: str
        Effective timestamp column
    natural_key_column: str
        Natural key column
    end_timestamp_column: Optional[str]
        End timestamp column
    source_expr: Select
        Source expression

    Returns
    -------
    Tuple[str, Select]
    """
    if end_timestamp_column is None:
        assert natural_key_column is not None
        scd_expr = augment_scd_table_with_end_timestamp(
            table_expr=source_expr,
            effective_timestamp_column=effective_timestamp_column,
            natural_key_column=natural_key_column,
        )
        end_timestamp_column = END_TS
    else:
        scd_expr = source_expr
        end_timestamp_column = end_timestamp_column
    return end_timestamp_column, scd_expr


def get_record_validity_condition(
    adapter: BaseAdapter,
    effective_timestamp_column: str,
    end_timestamp_column: str,
    point_in_time_expr: Expression,
) -> Expression:
    """
    Returns a condition to filter records that are valid as at point in time

    Parameters
    ----------
    adapter: BaseAdapter
        Sql adapter
    effective_timestamp_column: str
        Effective timestamp column
    end_timestamp_column: str
        End timestamp column
    point_in_time_expr: Expression
        Point in time expression

    Returns
    -------
    Expression
    """
    return expressions.and_(
        # SCD.effective_timestamp_column <= REQ.POINT_IN_TIME; i.e. record became effective
        # at or before point in time
        expressions.LTE(
            this=adapter.normalize_timestamp_before_comparison(
                get_qualified_column_identifier(effective_timestamp_column, "SCD")
            ),
            expression=point_in_time_expr,
        ),
        expressions.or_(
            # SCD.end_timestamp_column > REQ.POINT_IN_TIME; i.e. record has not yet been
            # invalidated as at the point in time, but will be at a future time
            expressions.GT(
                this=adapter.normalize_timestamp_before_comparison(
                    get_qualified_column_identifier(end_timestamp_column, "SCD")
                ),
                expression=point_in_time_expr,
            ),
            # SCD.end_timestamp_column IS NULL; i.e. record is current
            expressions.Is(
                this=get_qualified_column_identifier(end_timestamp_column, "SCD"),
                expression=expressions.Null(),
            ),
        ),
    )
