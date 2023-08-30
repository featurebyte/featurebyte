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
                  VECTOR_T0."serving_name" AS "serving_name",
                  VECTOR_T0."serving_name_2" AS "serving_name_2",
                  GROUP_BY_RESULT."sum_result" AS "sum_result",
                  GROUP_BY_RESULT."avg_result" AS "avg_result",
                  VECTOR_T0."result" AS "result",
                  VECTOR_T1."result2" AS "result2",
                  VECTOR_T2."result3" AS "result3"
                FROM (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_0."VECTOR_AGG_RESULT" AS "result"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent" AS parent
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_0"
                ) AS VECTOR_T0
                INNER JOIN (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_1."VECTOR_AGG_RESULT" AS "result2"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent2" AS parent2
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent2) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_1"
                ) AS VECTOR_T1
                  ON VECTOR_T0."serving_name" = VECTOR_T1."serving_name"
                  AND VECTOR_T0."serving_name_2" = VECTOR_T1."serving_name_2"
                INNER JOIN (
                  SELECT
                    INITIAL_DATA."serving_name" AS "serving_name",
                    INITIAL_DATA."serving_name_2" AS "serving_name_2",
                    AGG_2."VECTOR_AGG_RESULT" AS "result3"
                  FROM (
                    SELECT
                      REQ."serving_name" AS "serving_name",
                      REQ."serving_name_2" AS "serving_name_2",
                      TABLE."parent3" AS parent3
                  ) AS INITIAL_DATA, TABLE(
                    VECTOR_AGGREGATE_SUM(parent3) OVER (PARTITION BY INITIAL_DATA."serving_name", INITIAL_DATA."serving_name_2")
                  ) AS "AGG_2"
                ) AS VECTOR_T2
                  ON VECTOR_T1."serving_name" = VECTOR_T2."serving_name"
                  AND VECTOR_T1."serving_name_2" = VECTOR_T2."serving_name_2"
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
                  ON GROUP_BY_RESULT."serving_name" = VECTOR_T2."serving_name"
                  AND GROUP_BY_RESULT."serving_name_2" = VECTOR_T2."serving_name_2"
            """
        ).strip()
