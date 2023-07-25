"""
Unit tests for OnlineStoreCleanupService
"""
import textwrap

from featurebyte.service.online_store_cleanup import OnlineStoreCleanupService


def test_sql_query():
    """
    Test sql query for cleaning up online store tables
    """
    query = OnlineStoreCleanupService._get_cleanup_query("online_store_1")
    expected = textwrap.dedent(
        """
        MERGE INTO online_store_1 USING (
          SELECT
            "AGGREGATION_RESULT_NAME" AS "max_result_name",
            MAX("VERSION") AS "max_version"
          FROM online_store_1
          GROUP BY
            "AGGREGATION_RESULT_NAME"
        ) ON "AGGREGATION_RESULT_NAME" = "max_result_name" AND "VERSION" <= "max_version" - 2   WHEN MATCHED THEN DELETE
        """
    ).strip()
    assert query.sql(pretty=True) == expected
