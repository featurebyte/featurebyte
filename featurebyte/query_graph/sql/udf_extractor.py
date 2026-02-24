"""
UDF Extractor Module

This module provides functionality to find UDF references in sqlglot expressions.
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Expression


def extract_udfs_from_expression(
    expr: Expression,
    available_udfs: dict[str, str],
) -> set[str]:
    """
    Walk an expression tree and find references to UDFs.

    Parameters
    ----------
    expr: Expression
        The sqlglot expression to analyze
    available_udfs: dict[str, str]
        Mapping of available UDF names to their file paths

    Returns
    -------
    set[str]
        Set of matched UDF names (uppercase)
    """
    matched_udfs: set[str] = set()

    for node in expr.walk():
        udf_name = _extract_udf_name_from_node(node)
        if udf_name and udf_name.upper() in available_udfs:
            matched_udfs.add(udf_name.upper())

    return matched_udfs


def _extract_udf_name_from_node(node: Expression) -> str | None:
    """
    Extract a UDF name from a node if it represents a UDF call.

    UDFs appear as Anonymous nodes in sqlglot. For BigQuery with fully qualified
    names (e.g., `project.dataset.F_COUNT_DICT_ENTROPY`), the Anonymous node is
    nested inside Dot nodes, but walk() will still visit it.

    Parameters
    ----------
    node: Expression
        The sqlglot expression node to check

    Returns
    -------
    str | None
        The UDF name if found, None otherwise
    """
    if isinstance(node, expressions.Anonymous):
        func_name = node.this
        if isinstance(func_name, str) and func_name.upper().startswith("F_"):
            return func_name
        # Handle OBJECT_DELETE which doesn't have F_ prefix
        if isinstance(func_name, str) and func_name.upper() == "OBJECT_DELETE":
            return func_name

    return None
