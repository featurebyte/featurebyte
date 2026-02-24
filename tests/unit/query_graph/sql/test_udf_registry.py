"""
Tests for udf_registry module
"""

import os

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.common import JAR_PATH_PLACEHOLDER
from featurebyte.query_graph.sql.udf_registry import (
    SOURCE_TYPE_TO_SQL_DIRECTORY,
    SPARK_UDF_CLASS_MAPPING,
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

    def test_spark_udfs_are_jar_based(self):
        """Test that Spark UDFs return class names instead of file paths"""
        udfs = get_available_udfs(SourceType.SPARK)
        assert isinstance(udfs, dict)
        assert len(udfs) > 0
        # Check that common UDFs are present
        assert "F_COUNT_DICT_ENTROPY" in udfs
        assert "F_TIMESTAMP_TO_INDEX" in udfs
        # Values should be class names, not file paths
        assert udfs["F_COUNT_DICT_ENTROPY"] == "com.featurebyte.hive.udf.CountDictEntropyV3"
        assert udfs["F_TIMESTAMP_TO_INDEX"] == "com.featurebyte.hive.udf.TimestampToIndexV1"

    def test_databricks_udfs_are_jar_based(self):
        """Test that Databricks UDFs return class names instead of file paths"""
        udfs = get_available_udfs(SourceType.DATABRICKS)
        assert isinstance(udfs, dict)
        assert len(udfs) > 0
        # Should have same UDFs as Spark
        assert "F_COUNT_DICT_ENTROPY" in udfs
        assert udfs["F_COUNT_DICT_ENTROPY"] == "com.featurebyte.hive.udf.CountDictEntropyV3"

    def test_spark_udfs_match_class_mapping(self):
        """Test that all Spark UDFs in class mapping are available"""
        udfs = get_available_udfs(SourceType.SPARK)
        for udf_name, class_name in SPARK_UDF_CLASS_MAPPING.items():
            assert udf_name.upper() in udfs
            assert udfs[udf_name.upper()] == class_name


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

    def test_udf_dependencies_included(self):
        """Test that UDF dependencies are automatically included"""
        # F_COUNT_DICT_MOST_FREQUENT depends on F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"F_COUNT_DICT_MOST_FREQUENT"},
        )
        # Should include both the requested UDF and its dependency
        assert len(sqls) == 2
        # Check both UDFs are present
        sql_content = "\n".join(sqls)
        assert "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE" in sql_content
        assert "F_COUNT_DICT_MOST_FREQUENT" in sql_content

    def test_udf_dependencies_ordered_correctly(self):
        """Test that dependencies are ordered before dependents"""
        # F_COUNT_DICT_MOST_FREQUENT depends on F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"F_COUNT_DICT_MOST_FREQUENT"},
        )
        assert len(sqls) == 2
        # Dependency should come first
        assert "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE" in sqls[0]
        assert "F_COUNT_DICT_MOST_FREQUENT" in sqls[1]

    def test_multiple_udfs_with_shared_dependency(self):
        """Test multiple UDFs sharing the same dependency"""
        # Both F_COUNT_DICT_MOST_FREQUENT and F_COUNT_DICT_LEAST_FREQUENT
        # depend on F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE
        sqls = get_udf_registration_sql(
            source_type=SourceType.SNOWFLAKE,
            udf_names={"F_COUNT_DICT_MOST_FREQUENT", "F_COUNT_DICT_LEAST_FREQUENT"},
        )
        # Should include 3 UDFs: 2 requested + 1 shared dependency
        assert len(sqls) == 3
        # Dependency should come first
        assert "F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE" in sqls[0]

    def test_spark_udf_registration_generates_jar_sql(self):
        """Test Spark UDF registration generates JAR-based SQL"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SPARK,
            udf_names={"F_COUNT_DICT_ENTROPY"},
        )
        assert len(sqls) == 1
        assert "CREATE OR REPLACE FUNCTION F_COUNT_DICT_ENTROPY" in sqls[0]
        assert "AS 'com.featurebyte.hive.udf.CountDictEntropyV3'" in sqls[0]
        assert f"USING JAR '{JAR_PATH_PLACEHOLDER}'" in sqls[0]

    def test_databricks_udf_registration_generates_jar_sql(self):
        """Test Databricks UDF registration generates JAR-based SQL"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.DATABRICKS,
            udf_names={"F_TIMESTAMP_TO_INDEX"},
        )
        assert len(sqls) == 1
        assert "CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX" in sqls[0]
        assert "AS 'com.featurebyte.hive.udf.TimestampToIndexV1'" in sqls[0]
        assert f"USING JAR '{JAR_PATH_PLACEHOLDER}'" in sqls[0]

    def test_spark_multiple_udfs_registration(self):
        """Test Spark registration with multiple UDFs"""
        sqls = get_udf_registration_sql(
            source_type=SourceType.SPARK,
            udf_names={"F_COUNT_DICT_ENTROPY", "F_TIMESTAMP_TO_INDEX"},
        )
        assert len(sqls) == 2
        # Should be sorted by name
        assert "F_COUNT_DICT_ENTROPY" in sqls[0]
        assert "F_TIMESTAMP_TO_INDEX" in sqls[1]
        # Both should have JAR reference
        for sql in sqls:
            assert f"USING JAR '{JAR_PATH_PLACEHOLDER}'" in sql
