"""
Base adapter test class that can be reused across all adapters
"""

import textwrap
from typing import cast

from sqlglot import select
from sqlglot.expressions import Identifier, Select, alias_

from featurebyte import AggFunc
from featurebyte.enum import DBVarType
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.common import get_qualified_column_identifier, quoted_identifier
from featurebyte.query_graph.sql.groupby_helper import (
    GroupbyColumn,
    GroupbyKey,
    get_aggregation_expression,
    get_vector_agg_column_snowflake,
)


class BaseAdapterTest:
    """
    Base adapter test class that can be reused across all adapters
    """

    adapter: BaseAdapter
    expected_physical_type_from_dtype_mapping: dict[str, str] = {}

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
                  entity_column,
                  "entity_column_2",
                  SUM("parent") AS "sum_result",
                  AVG("parent_avg") AS "avg_result"
                GROUP BY
                  REQ."serving_name",
                  REQ."serving_name_2"
            """
        ).strip()

    def test_group_by(self):
        """
        Test group_by method
        """
        adapter = self.adapter
        select_expr: Select = cast(Select, select("a", "b"))
        groupby_keys = [
            GroupbyKey(
                expr=get_qualified_column_identifier("serving_name", "REQ"),
                name="serving_name",
            ),
            GroupbyKey(
                expr=get_qualified_column_identifier("serving_name_2", "REQ"),
                name="serving_name_2",
            ),
        ]
        select_keys = [k.get_alias() for k in groupby_keys]
        select_keys.append(Identifier(this="entity_column"))
        select_keys.append(quoted_identifier("entity_column_2"))
        keys = [k.expr for k in groupby_keys]
        agg_exprs = [
            alias_(
                get_aggregation_expression(AggFunc.SUM, "parent", None, self.adapter),
                alias="sum_result",
                quoted=True,
            ),
            alias_(
                get_aggregation_expression(AggFunc.AVG, "parent_avg", None, self.adapter),
                alias="avg_result",
                quoted=True,
            ),
        ]
        vector_aggregate_exprs = [
            get_vector_agg_column_snowflake(
                select(),
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result",
                    parent_cols=[(get_qualified_column_identifier("parent", "TABLE"))],
                ),
                0,
                False,
            ),
            get_vector_agg_column_snowflake(
                select(),
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent2", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result2",
                    parent_cols=[(get_qualified_column_identifier("parent2", "TABLE"))],
                ),
                1,
                False,
            ),
            get_vector_agg_column_snowflake(
                select(),
                AggFunc.SUM,
                groupby_keys,
                GroupbyColumn(
                    AggFunc.SUM,
                    parent_expr=(get_qualified_column_identifier("parent3", "TABLE")),
                    parent_dtype=DBVarType.ARRAY,
                    result_name="result3",
                    parent_cols=[(get_qualified_column_identifier("parent3", "TABLE"))],
                ),
                2,
                False,
            ),
        ]
        group_by_expr = adapter.group_by(
            select_expr, select_keys, agg_exprs, keys, vector_aggregate_exprs
        )
        expected = self.get_group_by_expected_result()
        assert group_by_expr.sql(pretty=True) == expected

    @classmethod
    def get_expected_haversine_sql(cls) -> str:
        """
        Get expected haversine SQL string
        """
        return textwrap.dedent(
            """
            2 * ASIN(
              SQRT(
                POWER(SIN((
                  RADIANS(TABLE."lat1") - RADIANS(TABLE."lat2")
                ) / 2), 2) + COS(RADIANS(TABLE."lat1")) * COS(RADIANS(TABLE."lat2")) * POWER(SIN((
                  RADIANS(TABLE."lon1") - RADIANS(TABLE."lon2")
                ) / 2), 2)
              )
            ) * 6371
        """
        ).strip()

    def test_haversine(self):
        adapter = self.adapter
        lat_node_1_expr = get_qualified_column_identifier("lat1", "TABLE")
        lon_node_1_expr = get_qualified_column_identifier("lon1", "TABLE")
        lat_node_2_expr = get_qualified_column_identifier("lat2", "TABLE")
        lon_node_2_expr = get_qualified_column_identifier("lon2", "TABLE")
        expr = adapter.haversine(lat_node_1_expr, lon_node_1_expr, lat_node_2_expr, lon_node_2_expr)
        assert expr.sql(pretty=True) == self.get_expected_haversine_sql()

    @classmethod
    def get_expected_deterministic_split_prob_sql(cls) -> str:
        """
        Get expected SQL for get_deterministic_split_prob_expr
        """
        return (
            "CAST(BITAND(HASH(CONCAT(CAST(\"__FB_TABLE_ROW_INDEX\" AS VARCHAR), '_42')), "
            "1073741823) AS DOUBLE) / 1073741824.0"
        )

    def test_deterministic_split_prob_expr(self):
        """
        Test get_deterministic_split_prob_expr produces correct SQL with bitmask normalization
        """
        adapter = self.adapter
        row_id_expr = quoted_identifier("__FB_TABLE_ROW_INDEX")
        expr = adapter.get_deterministic_split_prob_expr(row_id_expr, seed=42)
        assert expr.sql() == self.get_expected_deterministic_split_prob_sql()

    def test_deterministic_split_prob_expr_different_seeds(self):
        """
        Test that different seeds produce different SQL expressions
        """
        adapter = self.adapter
        row_id_expr = quoted_identifier("__FB_TABLE_ROW_INDEX")
        expr_seed_1 = adapter.get_deterministic_split_prob_expr(row_id_expr, seed=1)
        expr_seed_2 = adapter.get_deterministic_split_prob_expr(row_id_expr, seed=2)
        assert expr_seed_1.sql() != expr_seed_2.sql()
        assert "'_1'" in expr_seed_1.sql()
        assert "'_2'" in expr_seed_2.sql()

    def test_get_physical_type_from_dtype(self):
        """
        Test get_physical_type_from_dtype
        """
        adapter = self.adapter
        mapping = {}
        for dtype in DBVarType:
            mapping[dtype.value] = str(adapter.get_physical_type_from_dtype(dtype))
        assert mapping == self.expected_physical_type_from_dtype_mapping
