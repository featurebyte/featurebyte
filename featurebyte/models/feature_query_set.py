"""
FeatureQuerySet related classes
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot.expressions import Expression


@dataclass
class FeatureQuery:
    """
    FeatureQuery represents a sql query that materializes a temporary table for a set of features
    """

    sql: str | Expression
    table_name: str
    feature_names: list[str]


@dataclass
class FeatureQuerySet:
    """
    HistoricalFeatureQuerySet is a collection of FeatureQuery that materializes intermediate feature
    tables and a final query that joins them into one.
    """

    feature_queries: list[FeatureQuery]
    output_query: str | Expression
    output_table_name: str | None
    progress_message: str
    validate_output_row_index: bool = False
