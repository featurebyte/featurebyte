"""
Aggregate asat helper functions
"""

from __future__ import annotations

from typing import Optional, Tuple

from sqlglot import expressions
from sqlglot.expressions import Expression, Select

from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier
from featurebyte.query_graph.sql.scd_helper import END_TS, augment_scd_table_with_end_timestamp
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


def ensure_end_timestamp_column(
    adapter: BaseAdapter,
    effective_timestamp_column: str,
    effective_timestamp_schema: Optional[TimestampSchema],
    natural_key_column: Optional[str],
    end_timestamp_column: Optional[str],
    source_expr: Select,
) -> Tuple[str, Select]:
    """
    Ensure the end timestamp column is present in the source expression for SCD table

    Parameters
    ----------
    adapter: BaseAdapter
        Sql adapter
    effective_timestamp_column: str
        Effective timestamp column
    effective_timestamp_schema: Optional[TimestampSchema]
        Effective timestamp schema
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
            adapter=adapter,
            table_expr=source_expr,
            effective_timestamp_column=effective_timestamp_column,
            effective_timestamp_schema=effective_timestamp_schema,
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
    effective_timestamp_schema: Optional[TimestampSchema],
    end_timestamp_column: str,
    end_timestamp_schema: Optional[TimestampSchema],
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
    effective_timestamp_schema: Optional[TimestampSchema]
        Effective timestamp schema
    end_timestamp_column: str
        End timestamp column
    end_timestamp_schema: Optional[TimestampSchema]
        End timestamp schema
    point_in_time_expr: Expression
        Point in time expression

    Returns
    -------
    Expression
    """
    effective_timestamp_expr = get_qualified_column_identifier(effective_timestamp_column, "SCD")
    if effective_timestamp_schema is not None:
        effective_timestamp_expr = convert_timestamp_to_utc(
            effective_timestamp_expr, effective_timestamp_schema, adapter
        )

    end_timestamp_expr = get_qualified_column_identifier(end_timestamp_column, "SCD")
    if end_timestamp_schema is not None:
        end_timestamp_expr = convert_timestamp_to_utc(
            end_timestamp_expr, end_timestamp_schema, adapter
        )

    return expressions.and_(
        # SCD.effective_timestamp_column <= REQ.POINT_IN_TIME; i.e. record became effective
        # at or before point in time
        expressions.LTE(
            this=adapter.normalize_timestamp_before_comparison(effective_timestamp_expr),
            expression=point_in_time_expr,
        ),
        expressions.or_(
            # SCD.end_timestamp_column > REQ.POINT_IN_TIME; i.e. record has not yet been
            # invalidated as at the point in time, but will be at a future time
            expressions.GT(
                this=adapter.normalize_timestamp_before_comparison(end_timestamp_expr),
                expression=point_in_time_expr,
            ),
            # SCD.end_timestamp_column IS NULL; i.e. record is current
            expressions.Is(
                this=end_timestamp_expr,
                expression=expressions.Null(),
            ),
        ),
    )
