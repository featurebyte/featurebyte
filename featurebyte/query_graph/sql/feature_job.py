"""
Feature job utilities for sql generation
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.ast.literal import make_literal_value


def get_previous_job_epoch_expr_from_settings(
    point_in_time_epoch_expr: Expression,
    period_seconds: int,
    offset_seconds: int,
) -> Expression:
    """
    Get an expression for the epoch second of the latest feature job as of a point in time

    Parameters
    ----------
    point_in_time_epoch_expr: Expression
        Expression for point-in-time in epoch second
    period_seconds: int
        Feature job settings' period in seconds
    offset_seconds: int
        Feature job settings offset in seconds

    Returns
    -------
    Expression
    """
    frequency = make_literal_value(period_seconds)
    time_modulo_frequency = make_literal_value(offset_seconds)

    # FLOOR((POINT_IN_TIME - TIME_MODULO_FREQUENCY) / FREQUENCY)
    previous_job_index_expr = expressions.Floor(
        this=expressions.Div(
            this=expressions.Paren(
                this=expressions.Sub(
                    this=point_in_time_epoch_expr, expression=time_modulo_frequency
                )
            ),
            expression=frequency,
        )
    )

    # PREVIOUS_JOB_INDEX * FREQUENCY + TIME_MODULO_FREQUENCY
    previous_job_epoch_expr = expressions.Add(
        this=expressions.Mul(this=previous_job_index_expr, expression=frequency),
        expression=time_modulo_frequency,
    )

    return previous_job_epoch_expr
