"""
Test snowflake adapter module
"""
import textwrap

from featurebyte.query_graph.sql.adapter import SnowflakeAdapter
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest


class TestSnowflakeAdapter(BaseAdapterTest):
    """
    Test snowflake adapter class
    """

    adapter = SnowflakeAdapter()

    @classmethod
    def get_group_by_expected_result(cls) -> str:
        """
        Returns expected result of group by query
        """
        return textwrap.dedent(
            """
                SELECT
                  a,
                  b,
                  REQ."serving_name" AS "serving_name",
                  REQ."serving_name_2" AS "serving_name_2",
                  GROUP_BY_RESULT."sum_result" AS "sum_result",
                  GROUP_BY_RESULT."avg_result" AS "avg_result",
                  T0.result AS "result",
                  T1.result2 AS "result2",
                  T2.result3 AS "result3"
                FROM (
                  SELECT
                    REQ."serving_name" AS "serving_name",
                    REQ."serving_name_2" AS "serving_name_2",
                    AGG_0.VECTOR_AGG_RESULT AS "result"
                  FROM REQ, TABLE(
                    VECTOR_AGGREGATE_SUM(TABLE."parent") OVER (PARTITION BY REQ."serving_name", REQ."serving_name_2")
                  ) AS "AGG_0"
                ) AS T0
                INNER JOIN (
                  SELECT
                    REQ."serving_name" AS "serving_name",
                    REQ."serving_name_2" AS "serving_name_2",
                    AGG_1.VECTOR_AGG_RESULT AS "result2"
                  FROM REQ, TABLE(
                    VECTOR_AGGREGATE_SUM(TABLE."parent2") OVER (PARTITION BY REQ."serving_name", REQ."serving_name_2")
                  ) AS "AGG_1"
                ) AS T1
                  ON T0."serving_name" = T1."serving_name" AND T0."serving_name_2" = T1."serving_name_2"
                INNER JOIN (
                  SELECT
                    REQ."serving_name" AS "serving_name",
                    REQ."serving_name_2" AS "serving_name_2",
                    AGG_2.VECTOR_AGG_RESULT AS "result3"
                  FROM REQ, TABLE(
                    VECTOR_AGGREGATE_SUM(TABLE."parent3") OVER (PARTITION BY REQ."serving_name", REQ."serving_name_2")
                  ) AS "AGG_2"
                ) AS T2
                  ON T1."serving_name" = T2."serving_name" AND T1."serving_name_2" = T2."serving_name_2"
                INNER JOIN (
                  SELECT
                    a,
                    b,
                    REQ."serving_name" AS "serving_name",
                    REQ."serving_name_2" AS "serving_name_2",
                    SUM("parent") AS "sum_result",
                    AVG("parent_avg") AS "avg_result"
                  GROUP BY
                    REQ."serving_name",
                    REQ."serving_name_2"
                ) AS GROUP_BY_RESULT
                  ON GROUP_BY_RESULT."serving_name" = T2."serving_name"
                  AND GROUP_BY_RESULT."serving_name_2" = T2."serving_name_2"
            """
        ).strip()
