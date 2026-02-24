"""
UDF Registry Module

This module provides functionality to identify available UDFs for each source type
and generate UDF registration SQL statements.
"""

from __future__ import annotations

import os
from typing import Optional

from featurebyte.common.path_util import get_package_root
from featurebyte.enum import SourceType

# Mapping from SourceType to SQL directory name
SOURCE_TYPE_TO_SQL_DIRECTORY: dict[SourceType, Optional[str]] = {
    SourceType.SNOWFLAKE: "snowflake",
    SourceType.BIGQUERY: "bigquery",
    SourceType.DATABRICKS_UNITY: "databricks_unity",
    SourceType.SPARK: "spark",
    SourceType.DATABRICKS: "spark",
}


def get_available_udfs(source_type: SourceType) -> dict[str, str]:
    """
    Scan the SQL directory for a source type and return available UDFs.

    Parameters
    ----------
    source_type: SourceType
        The database source type

    Returns
    -------
    dict[str, str]
        Mapping of UDF name (uppercase) to file path
    """
    sql_directory = SOURCE_TYPE_TO_SQL_DIRECTORY.get(source_type)
    if sql_directory is None:
        return {}

    sql_path = os.path.join(get_package_root(), "sql", sql_directory)
    if not os.path.isdir(sql_path):
        return {}

    udfs: dict[str, str] = {}
    for filename in os.listdir(sql_path):
        if not filename.endswith(".sql") or not filename.startswith("F_"):
            continue

        full_path = os.path.join(sql_path, filename)
        identifier = filename.replace(".sql", "")

        # OBJECT_DELETE does not include "F_" prefix per the existing convention
        if identifier == "F_OBJECT_DELETE":
            identifier = "OBJECT_DELETE"

        udfs[identifier.upper()] = full_path

    return udfs


def get_udf_registration_sql(
    source_type: SourceType,
    udf_names: set[str],
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
) -> list[str]:
    """
    Generate UDF registration SQL statements for the requested UDFs.

    Parameters
    ----------
    source_type: SourceType
        The database source type
    udf_names: set[str]
        Set of UDF names to generate SQL for
    database_name: Optional[str]
        Database name for substitution (BigQuery only)
    schema_name: Optional[str]
        Schema name for substitution (BigQuery only)

    Returns
    -------
    list[str]
        List of CREATE FUNCTION statements
    """
    if not udf_names:
        return []

    available_udfs = get_available_udfs(source_type)
    registration_sqls: list[str] = []

    for udf_name in sorted(udf_names):
        udf_name_upper = udf_name.upper()
        if udf_name_upper not in available_udfs:
            continue

        file_path = available_udfs[udf_name_upper]
        with open(file_path, "r") as f:
            sql_content = f.read()

        # For BigQuery, substitute placeholders
        if source_type == SourceType.BIGQUERY and database_name and schema_name:
            sql_content = sql_content.replace("{project}", database_name)
            sql_content = sql_content.replace("{dataset}", schema_name)

        registration_sqls.append(sql_content.strip())

    return registration_sqls
