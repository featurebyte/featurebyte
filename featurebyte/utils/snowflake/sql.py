"""
Utility functions for sql string related changes
"""

from __future__ import annotations

from featurebyte.query_graph.sql.common import quoted_identifier


def escape_column_names(column_names: list[str]) -> list[str]:
    """Enclose provided column names with quotes

    Parameters
    ----------
    column_names : list[str]
        Column names

    Returns
    -------
    list[str]
    """
    return [quoted_identifier(x).sql() for x in column_names]
