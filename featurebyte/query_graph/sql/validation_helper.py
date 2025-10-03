"""
Helpers for generating SQL queries for table validation.
"""

from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier


def _exclude_null_values(source_expr: Select, column: str) -> Select:
    return source_expr.where(
        expressions.Is(
            this=quoted_identifier(column),
            expression=expressions.Not(this=expressions.Null()),
        )
    )


def get_duplicate_rows_per_keys(
    source_expr: Select,
    key_columns: list[str],
    exclude_null_column: str,
    count_output_column_name: str,
    num_records_to_retrieve: int,
) -> Select:
    source_expr = _exclude_null_values(source_expr, exclude_null_column)
    query_expr = (
        select(
            *[quoted_identifier(col) for col in key_columns],
            expressions.alias_(
                expressions.Count(this=expressions.Star()),
                alias=count_output_column_name,
                quoted=True,
            ),
        )
        .from_(source_expr.subquery())
        .group_by(*[quoted_identifier(col) for col in key_columns])
        .having(
            expressions.GT(
                this=quoted_identifier(count_output_column_name),
                expression=make_literal_value(1),
            )
        )
        .limit(num_records_to_retrieve)
    )
    return query_expr
