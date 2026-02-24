"""
UDF Extractor Module

This module provides functionality to find UDF references in sqlglot expressions.
"""

from __future__ import annotations

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.enum import SourceType


def extract_udfs_from_expression(
    expr: Expression,
    available_udfs: dict[str, str],
    source_type: SourceType,
) -> set[str]:
    """
    Walk an expression tree and find references to UDFs.

    Parameters
    ----------
    expr: Expression
        The sqlglot expression to analyze
    available_udfs: dict[str, str]
        Mapping of available UDF names to their file paths
    source_type: SourceType
        The database source type

    Returns
    -------
    set[str]
        Set of matched UDF names (uppercase)
    """
    matched_udfs: set[str] = set()

    for node in expr.walk():
        udf_name = _extract_udf_name_from_node(node, source_type)
        if udf_name and udf_name.upper() in available_udfs:
            matched_udfs.add(udf_name.upper())

    return matched_udfs


def _extract_udf_name_from_node(node: Expression, source_type: SourceType) -> str | None:
    """
    Extract a UDF name from a node if it represents a UDF call.

    For standard databases (Snowflake, Databricks, Spark), UDFs appear as:
        Anonymous(this="F_COUNT_DICT_ENTROPY", ...)

    For BigQuery, UDFs appear as fully qualified names:
        Dot(Dot(...), Anonymous(this="F_COUNT_DICT_ENTROPY", ...))

    Parameters
    ----------
    node: Expression
        The sqlglot expression node to check
    source_type: SourceType
        The database source type

    Returns
    -------
    str | None
        The UDF name if found, None otherwise
    """
    if source_type == SourceType.BIGQUERY:
        # For BigQuery, look for Dot nodes containing Anonymous for fully qualified names
        # Pattern: Dot(Dot(Var, Var), Anonymous(this="F_..."))
        if isinstance(node, expressions.Dot):
            # The function call is in the expression part of the Dot
            inner = node.expression
            if isinstance(inner, expressions.Anonymous):
                func_name = inner.this
                if isinstance(func_name, str) and func_name.upper().startswith("F_"):
                    return func_name
                # Handle OBJECT_DELETE which doesn't have F_ prefix
                if isinstance(func_name, str) and func_name.upper() == "OBJECT_DELETE":
                    return func_name
    else:
        # For standard databases, look for Anonymous nodes directly
        if isinstance(node, expressions.Anonymous):
            func_name = node.this
            if isinstance(func_name, str) and func_name.upper().startswith("F_"):
                return func_name
            # Handle OBJECT_DELETE which doesn't have F_ prefix
            if isinstance(func_name, str) and func_name.upper() == "OBJECT_DELETE":
                return func_name

    return None
