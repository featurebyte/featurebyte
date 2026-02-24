"""
Tests for udf_registry module
"""

import os

import pytest

from featurebyte.common.path_util import get_package_root
from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.udf_registry import (
    SOURCE_TYPE_TO_SQL_DIRECTORY,
    get_available_udfs,
    get_udf_registration_sql,
)


class TestSourceTypeToSqlDirectory:
    """Tests for SOURCE_TYPE_TO_SQL_DIRECTORY mapping"""

    def test_snowflake_mapping(self):
        """Test Snowflake mapping"""
        assert SOURCE_TYPE_TO_SQL_DIRECTORY[SourceType.SNOWFLAKE] == "snowflake"

    def test_bigquery_mapping(self):
        """Test BigQuery mapping"""
        assert SOURCE_TYPE_TO_SQL_DIRECTORY[SourceType.BIGQUERY] == "bigquery"

    def test_databricks_unity_mapping(self):
        """Test Databricks Unity mapping"""
        assert SOURCE_TYPE_TO_SQL_DIRECTORY[SourceType.DATABRICKS_UNITY] == "databricks_unity"

    def test_spark_mapping(self):
        """Test Spark mapping"""
        assert SOURCE_TYPE_TO_SQL_DIRECTORY[SourceType.SPARK] == "spark"

    def test_databricks_mapping(self):
        """Test Databricks maps to spark"""
        assert SOURCE_TYPE_TO_SQL_DIRECTORY[SourceType.DATABRICKS] == "spark"


class TestGetAvailableUdfs:
    """Tests for get_available_udfs function"""

    def test_snowflake_udfs(self):
        """Test getting available UDFs for Snowflake"""
        udfs = get_available_udfs(SourceType.SNOWFLAKE)
        assert isinstance(udfs, dict)
        assert len(udfs) > 0
        # Check that common UDFs are present
        assert "F_COUNT_DICT_ENTROPY" in udfs
        assert "F_TIMESTAMP_TO_INDEX" in udfs

    def test_bigquery_udfs(self):
        """Test getting available UDFs for BigQuery"""
        udfs = get_available_udfs(SourceType.BIGQUERY)
        assert isinstance(udfs, dict)
        assert len(udfs) > 0
        assert "F_COUNT_DICT_ENTROPY" in udfs

    def test_databricks_unity_udfs(self):
        """Test getting available UDFs for Databricks Unity"""
        udfs = get_available_udfs(SourceType.DATABRICKS_UNITY)
        assert isinstance(udfs, dict)
        assert len(udfs) > 0
        assert "F_COUNT_DICT_ENTROPY" in udfs

    def test_unsupported_source_type(self):
        """Test unsupported source type returns empty dict"""
        udfs = get_available_udfs(SourceType.SQLITE)
        assert udfs == {}

    def test_udf_paths_exist(self):
        """Test that returned UDF paths exist"""
        udfs = get_available_udfs(SourceType.SNOWFLAKE)
        for udf_name, file_path in udfs.items():
            assert os.path.isfile(file_path), f"UDF file for {udf_name} does not exist: {file_path}"

    def test_object_delete_special_case(self):
        """Test that OBJECT_DELETE is correctly mapped without F_ prefix"""
        # OBJECT_DELETE exists in BigQuery and Databricks Unity, not Snowflake
        udfs = get_available_udfs(SourceType.BIGQUERY)
        # Should NOT have F_OBJECT_DELETE
        assert "F_OBJECT_DELETE" not in udfs
        # Should have OBJECT_DELETE
        assert "OBJECT_DELETE" in udfs
        # The file path should end with F_OBJECT_DELETE.sql
        assert udfs["OBJECT_DELETE"].endswith("F_OBJECT_DELETE.sql")


class TestGetUdfRegistrationSql:
    """Tests for get_udf_registration_sql function"""

    def test_snowflake_udf_registration(self):
        """Test generating UDF registration SQL for Snowflake"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"F_COUNT_DICT_ENTROPY"},
        )
        assert len(sqls) == 1
        assert "CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY" in sqls[0]

    def test_bigquery_udf_registration_with_placeholders(self):
        """Test BigQuery UDF registration with project/dataset substitution"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.BIGQUERY,
            udf_names={"F_COUNT_DICT_ENTROPY"},
            database_name="my_project",
            schema_name="my_dataset",
        )
        assert len(sqls) == 1
        assert "my_project" in sqls[0]
        assert "my_dataset" in sqls[0]
        assert "{project}" not in sqls[0]
        assert "{dataset}" not in sqls[0]

    def test_bigquery_udf_registration_without_substitution(self):
        """Test BigQuery UDF registration keeps placeholders without database/schema"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.BIGQUERY,
            udf_names={"F_COUNT_DICT_ENTROPY"},
        )
        assert len(sqls) == 1
        # Placeholders remain when database_name/schema_name not provided
        assert "{project}" in sqls[0]
        assert "{dataset}" in sqls[0]

    def test_empty_udf_names(self):
        """Test empty UDF names returns empty list"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names=set(),
        )
        assert sqls == []

    def test_nonexistent_udf(self):
        """Test nonexistent UDF is skipped"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"NONEXISTENT_UDF"},
        )
        assert sqls == []

    def test_multiple_udfs(self):
        """Test multiple UDFs are returned in sorted order"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"F_COUNT_DICT_ENTROPY", "F_TIMESTAMP_TO_INDEX"},
        )
        assert len(sqls) == 2
        # Should be sorted by name
        assert "F_COUNT_DICT_ENTROPY" in sqls[0]
        assert "F_TIMESTAMP_TO_INDEX" in sqls[1]

    def test_case_insensitivity(self):
        """Test UDF name matching is case insensitive"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"f_count_dict_entropy"},  # lowercase
        )
        assert len(sqls) == 1
        assert "F_COUNT_DICT_ENTROPY" in sqls[0]
