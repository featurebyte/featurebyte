"""
Tests for udf_extractor module
"""

import pytest
import sqlglot

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.udf_extractor import extract_udfs_from_expression


class TestExtractUdfsFromExpression:
    """Tests for extract_udfs_from_expression function"""

    @pytest.fixture
    def snowflake_available_udfs(self):
        """Sample available UDFs for testing"""
        return {
            "F_COUNT_DICT_ENTROPY": "/path/to/F_COUNT_DICT_ENTROPY.sql",
            "F_TIMESTAMP_TO_INDEX": "/path/to/F_TIMESTAMP_TO_INDEX.sql",
            "F_GET_VALUE": "/path/to/F_GET_VALUE.sql",
            "OBJECT_DELETE": "/path/to/F_OBJECT_DELETE.sql",
        }

    def test_simple_udf_reference(self, snowflake_available_udfs):
        """Test detecting a simple UDF reference"""
        sql = "SELECT F_COUNT_DICT_ENTROPY(counts) FROM table1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_multiple_udf_references(self, snowflake_available_udfs):
        """Test detecting multiple UDF references"""
        sql = """
        SELECT
            F_COUNT_DICT_ENTROPY(counts) AS entropy,
            F_TIMESTAMP_TO_INDEX(ts) AS idx
        FROM table1
        """
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY", "F_TIMESTAMP_TO_INDEX"}

    def test_nested_udf_references(self, snowflake_available_udfs):
        """Test detecting nested UDF references"""
        sql = "SELECT F_GET_VALUE(F_COUNT_DICT_ENTROPY(counts), 'key') FROM table1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY", "F_GET_VALUE"}

    def test_no_udf_references(self, snowflake_available_udfs):
        """Test SQL without UDF references"""
        sql = "SELECT col1, SUM(col2) FROM table1 GROUP BY col1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == set()

    def test_udf_not_in_available_list(self, snowflake_available_udfs):
        """Test UDF not in available list is not detected"""
        sql = "SELECT F_UNKNOWN_UDF(col1) FROM table1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == set()

    def test_object_delete_special_case(self, snowflake_available_udfs):
        """Test OBJECT_DELETE without F_ prefix is detected"""
        sql = "SELECT OBJECT_DELETE(obj, 'key') FROM table1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"OBJECT_DELETE"}

    def test_bigquery_fully_qualified_udf(self):
        """Test detecting UDF with fully qualified name for BigQuery"""
        available_udfs = {
            "F_COUNT_DICT_ENTROPY": "/path/to/F_COUNT_DICT_ENTROPY.sql",
        }
        sql = "SELECT `project`.`dataset`.F_COUNT_DICT_ENTROPY(counts) FROM table1"
        expr = sqlglot.parse_one(sql, read="bigquery")
        udfs = extract_udfs_from_expression(expr, available_udfs, SourceType.BIGQUERY)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_bigquery_multiple_qualified_udfs(self):
        """Test detecting multiple fully qualified UDFs for BigQuery"""
        available_udfs = {
            "F_COUNT_DICT_ENTROPY": "/path/to/F_COUNT_DICT_ENTROPY.sql",
            "F_GET_VALUE": "/path/to/F_GET_VALUE.sql",
        }
        sql = """
        SELECT
            `project`.`dataset`.F_COUNT_DICT_ENTROPY(counts) AS entropy,
            `project`.`dataset`.F_GET_VALUE(obj, 'key') AS val
        FROM table1
        """
        expr = sqlglot.parse_one(sql, read="bigquery")
        udfs = extract_udfs_from_expression(expr, available_udfs, SourceType.BIGQUERY)
        assert udfs == {"F_COUNT_DICT_ENTROPY", "F_GET_VALUE"}

    def test_case_insensitive_matching(self, snowflake_available_udfs):
        """Test that UDF matching is case insensitive"""
        sql = "SELECT f_count_dict_entropy(counts) FROM table1"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_udf_in_subquery(self, snowflake_available_udfs):
        """Test detecting UDF in subquery"""
        sql = """
        SELECT * FROM (
            SELECT F_COUNT_DICT_ENTROPY(counts) AS entropy FROM table1
        ) subq
        """
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_udf_in_cte(self, snowflake_available_udfs):
        """Test detecting UDF in CTE"""
        sql = """
        WITH cte AS (
            SELECT F_COUNT_DICT_ENTROPY(counts) AS entropy FROM table1
        )
        SELECT * FROM cte
        """
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_udf_in_where_clause(self, snowflake_available_udfs):
        """Test detecting UDF in WHERE clause"""
        sql = "SELECT * FROM table1 WHERE F_COUNT_DICT_ENTROPY(counts) > 0.5"
        expr = sqlglot.parse_one(sql, read="snowflake")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SNOWFLAKE)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_databricks_source_type(self, snowflake_available_udfs):
        """Test UDF detection for Databricks source type"""
        sql = "SELECT F_COUNT_DICT_ENTROPY(counts) FROM table1"
        expr = sqlglot.parse_one(sql, read="databricks")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.DATABRICKS)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}

    def test_spark_source_type(self, snowflake_available_udfs):
        """Test UDF detection for Spark source type"""
        sql = "SELECT F_COUNT_DICT_ENTROPY(counts) FROM table1"
        expr = sqlglot.parse_one(sql, read="spark")
        udfs = extract_udfs_from_expression(expr, snowflake_available_udfs, SourceType.SPARK)
        assert udfs == {"F_COUNT_DICT_ENTROPY"}
