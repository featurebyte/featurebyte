"""
Tests for featurebyte.query_graph.feature_compute
"""

import copy
import textwrap
from dataclasses import asdict

import pytest
from bson import ObjectId
from sqlglot import expressions, select
from sqlglot.expressions import Identifier

from featurebyte.enum import AggFunc, DBVarType
from featurebyte.models.parent_serving import (
    EntityLookupInfo,
    EntityLookupStep,
    EntityRelationshipsContext,
)
from featurebyte.query_graph.node.generic import ItemGroupbyParameters
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.aggregator.request_table import RequestTablePlan
from featurebyte.query_graph.sql.aggregator.window import TileBasedRequestTablePlan
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME, quoted_identifier
from featurebyte.query_graph.sql.feature_compute import FeatureExecutionPlanner, FeatureQueryPlan
from featurebyte.query_graph.sql.specs import (
    AggregationSource,
    FeatureSpec,
    ItemAggregationSpec,
    TileBasedAggregationSpec,
)
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    feature_query_to_string,
)


@pytest.fixture(name="agg_spec_template")
def agg_spec_template_fixture(expected_pruned_graph_and_node_1):
    """Fixture for an AggregationSpec"""
    agg_spec = TileBasedAggregationSpec(
        node_name=expected_pruned_graph_and_node_1["pruned_node"].name,
        window=86400,
        offset=None,
        frequency=3600,
        blind_spot=120,
        time_modulo_frequency=1800,
        tile_table_id="some_tile_id",
        aggregation_id="some_agg_id",
        keys=["CUST_ID"],
        serving_names=["CID"],
        serving_names_mapping=None,
        value_by=None,
        merge_expr=expressions.Sum(this=Identifier(this="value")),
        feature_name="Amount (1d sum)",
        is_order_dependent=False,
        tile_value_columns=["value"],
        entity_ids=[ObjectId()],
        dtype=DBVarType.FLOAT,
        agg_func=AggFunc.SUM,
        agg_result_name_include_serving_names=True,
        **expected_pruned_graph_and_node_1,
    )
    return agg_spec


@pytest.fixture(name="agg_spec_sum_1d")
def agg_spec_sum_1d_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 1 day sum"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.tile_table_id = "sum_1d_tile_id"
    return agg_spec


@pytest.fixture(name="agg_spec_max_1d")
def agg_spec_max_1d_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 1 day max"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.merge_expr = "MAX(value)"
    agg_spec.tile_table_id = "max_1d_tile_id"
    return agg_spec


@pytest.fixture(name="agg_spec_max_2h")
def agg_spec_max_2h_fixture(agg_spec_template):
    """Fixture for an AggregationSpec with 2 hour max"""
    agg_spec = copy.deepcopy(agg_spec_template)
    agg_spec.window = 7200
    agg_spec.merge_expr = "MAX(value)"
    agg_spec.tile_table_id = "max_2h_tile_id"
    return agg_spec


@pytest.fixture(name="item_agg_spec")
def item_agg_spec_fixture():
    parameters = ItemGroupbyParameters(
        keys=["order_id"],
        serving_names=["OID"],
        name="Order Size",
        agg_func="count",
    )
    agg_spec = ItemAggregationSpec(
        node_name="item_groupby_1",
        feature_name=parameters.name,
        parameters=parameters,
        serving_names=["OID"],
        serving_names_mapping=None,
        aggregation_source=AggregationSource(
            expr=select("*").from_("tab"), query_node_name="input_1"
        ),
        entity_ids=[ObjectId()],
        parent_dtype=DBVarType.FLOAT,
        agg_result_name_include_serving_names=True,
    )
    return agg_spec


def assert_sql_equal(sql, expected):
    """Helper function to check that SQL code matches with expected"""
    sql = textwrap.dedent(sql).strip()
    expected = textwrap.dedent(expected).strip()
    assert sql == expected


def test_request_table_plan__share_expanded_table(agg_spec_sum_1d, agg_spec_max_1d, source_info):
    """Test that two compatible AggregationSpec shares the same expanded request table"""
    plan = TileBasedRequestTablePlan(source_info=source_info)
    plan.add_aggregation_spec(agg_spec_sum_1d)
    plan.add_aggregation_spec(agg_spec_max_1d)

    assert (
        plan.get_expanded_request_table_name(agg_spec_sum_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )

    ctes = plan.construct_request_tile_indices_ctes(request_table_name=REQUEST_TABLE_NAME)
    assert len(ctes) == 1

    cte = ctes[0]
    assert cte.name == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    assert cte.quoted is True
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) - 24 AS __FB_FIRST_TILE_INDEX
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(cte.expr.sql(pretty=True), expected_sql)


def test_request_table_plan__no_sharing(agg_spec_max_2h, agg_spec_max_1d, source_info):
    """Test that two incompatible AggregationSpec does not share expanded request tables"""
    plan = TileBasedRequestTablePlan(source_info=source_info)
    plan.add_aggregation_spec(agg_spec_max_2h)
    plan.add_aggregation_spec(agg_spec_max_1d)

    assert (
        plan.get_expanded_request_table_name(agg_spec_max_2h)
        == "REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"
    )
    assert (
        plan.get_expanded_request_table_name(agg_spec_max_1d)
        == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    )

    ctes = plan.construct_request_tile_indices_ctes(request_table_name=REQUEST_TABLE_NAME)
    assert len(ctes) == 2

    # check expanded table for 2h
    name, sql = ctes[0].name, ctes[0].expr
    assert name == "REQUEST_TABLE_W7200_F3600_BS120_M1800_CID"
    assert ctes[0].quoted is True
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)

    # check expanded table for 1d
    name, sql = ctes[1].name, ctes[1].expr
    assert name == "REQUEST_TABLE_W86400_F3600_BS120_M1800_CID"
    assert ctes[1].quoted is True
    expected_sql = """
    SELECT
      "POINT_IN_TIME",
      "CID",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
      ) / 3600) AS BIGINT) - 24 AS __FB_FIRST_TILE_INDEX
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "CID"
      FROM REQUEST_TABLE
    )
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)


def test_non_time_aware_request_table_plan(item_agg_spec):
    """
    Test NonTimeAwareRequestTablePlan
    """
    plan = RequestTablePlan(is_time_aware=False)
    plan.add_aggregation_spec(item_agg_spec)
    assert plan.get_request_table_name(item_agg_spec) == "REQUEST_TABLE_OID"
    ctes = plan.construct_request_table_ctes(REQUEST_TABLE_NAME)
    assert len(ctes) == 1
    name, sql = ctes[0].name, ctes[0].expr  # noqa: F841
    expected_sql = """
    SELECT DISTINCT
      "OID"
    FROM REQUEST_TABLE
    """
    assert_sql_equal(sql.sql(pretty=True), expected_sql)


def test_feature_execution_planner(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
    source_info,
):
    """Test FeatureExecutionPlanner generates the correct plan from groupby node"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby, source_info=source_info, is_online_serving=False
    )
    plan = planner.generate_plan([groupby_node])
    actual = list(
        plan.aggregators["window"].window_aggregation_spec_set.get_grouped_aggregation_specs()
    )
    expected = [
        [
            TileBasedAggregationSpec(
                node_name=groupby_node.name,
                window=7200,
                offset=None,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                serving_names_mapping=None,
                value_by=None,
                merge_expr=expressions.Div(
                    this=expressions.Sum(
                        this=Identifier(
                            this=f"sum_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                    expression=expressions.Sum(
                        this=Identifier(
                            this=f"count_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                ),
                feature_name="a_2h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
                dtype=DBVarType.FLOAT,
                agg_func=AggFunc.AVG,
                agg_result_name_include_serving_names=True,
                **expected_pruned_graph_and_node_1,
            )
        ],
        [
            TileBasedAggregationSpec(
                node_name=groupby_node.name,
                window=172800,
                offset=None,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["CUSTOMER_ID"],
                serving_names_mapping=None,
                value_by=None,
                merge_expr=expressions.Div(
                    this=expressions.Sum(
                        this=Identifier(
                            this=f"sum_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                    expression=expressions.Sum(
                        this=Identifier(
                            this=f"count_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                ),
                feature_name="a_48h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
                dtype=DBVarType.FLOAT,
                agg_func=AggFunc.AVG,
                agg_result_name_include_serving_names=True,
                **expected_pruned_graph_and_node_2,
            )
        ],
    ]
    assert actual == expected
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_CUSTOMER_ID_window_w7200_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_CUSTOMER_ID_window_w172800_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
    }
    assert plan.required_entity_ids == {ObjectId("637516ebc9c18f5a277a78db")}


def test_feature_execution_planner__serving_names_mapping(
    query_graph_with_groupby,
    groupby_node_aggregation_id,
    expected_pruned_graph_and_node_1,
    expected_pruned_graph_and_node_2,
    source_info,
):
    """Test FeatureExecutionPlanner with serving names mapping provided"""
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    mapping = {"CUSTOMER_ID": "NEW_CUST_ID"}
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby,
        serving_names_mapping=mapping,
        source_info=source_info,
        is_online_serving=False,
    )
    plan = planner.generate_plan([groupby_node])
    assert list(
        plan.aggregators["window"].window_aggregation_spec_set.get_grouped_aggregation_specs()
    ) == [
        [
            TileBasedAggregationSpec(
                node_name=groupby_node.name,
                window=7200,
                offset=None,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                serving_names_mapping=mapping,
                value_by=None,
                merge_expr=expressions.Div(
                    this=expressions.Sum(
                        this=Identifier(
                            this=f"sum_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                    expression=expressions.Sum(
                        this=Identifier(
                            this=f"count_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                ),
                feature_name="a_2h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
                dtype=DBVarType.FLOAT,
                agg_func=AggFunc.AVG,
                agg_result_name_include_serving_names=True,
                **expected_pruned_graph_and_node_1,
            )
        ],
        [
            TileBasedAggregationSpec(
                node_name=groupby_node.name,
                window=172800,
                offset=None,
                frequency=3600,
                blind_spot=900,
                time_modulo_frequency=1800,
                tile_table_id="TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
                aggregation_id=f"avg_{groupby_node_aggregation_id}",
                keys=["cust_id"],
                serving_names=["NEW_CUST_ID"],
                serving_names_mapping=mapping,
                value_by=None,
                merge_expr=expressions.Div(
                    this=expressions.Sum(
                        this=Identifier(
                            this=f"sum_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                    expression=expressions.Sum(
                        this=Identifier(
                            this=f"count_value_avg_{groupby_node_aggregation_id}",
                        )
                    ),
                ),
                feature_name="a_48h_average",
                is_order_dependent=False,
                tile_value_columns=[
                    f"sum_value_avg_{groupby_node_aggregation_id}",
                    f"count_value_avg_{groupby_node_aggregation_id}",
                ],
                entity_ids=[ObjectId("637516ebc9c18f5a277a78db")],
                dtype=DBVarType.FLOAT,
                agg_func=AggFunc.AVG,
                agg_result_name_include_serving_names=True,
                **expected_pruned_graph_and_node_2,
            )
        ],
    ]
    assert plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_NEW_CUST_ID_window_w7200_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_NEW_CUST_ID_window_w172800_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
    }


def test_feature_execution_planner__lookup_features(
    global_graph, projected_lookup_features, source_info
):
    """
    Test FeatureExecutionPlanner on an LookupFeature node
    """
    mapping = {"cust_id": "CUSTOMER_ID"}
    planner = FeatureExecutionPlanner(
        global_graph,
        serving_names_mapping=mapping,
        source_info=source_info,
        is_online_serving=False,
    )
    nodes = list(projected_lookup_features)
    plan = planner.generate_plan(nodes)
    aggregator = plan.aggregators["lookup"]

    # Check aggregation results
    agg_results = aggregator.get_direct_lookups()
    assert len(agg_results) == 1
    agg_result_dict = asdict(agg_results[0])
    agg_result_dict.pop("expr")
    agg_result_dict.pop("adapter")
    assert agg_result_dict == {
        "column_names": [
            "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_1",
            "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_1",
        ],
        "join_keys": ["CUSTOMER_ID"],
        "forward_point_in_time_offset": None,
        "event_timestamp_column": None,
    }

    assert plan.required_entity_ids == {ObjectId("63dbe68cd918ef71acffd127")}


def test_feature_execution_planner__query_graph_with_graph_node(
    query_graph_with_cleaning_ops_and_groupby, source_info
):
    """Test FeatureExecutionPlanner generates the plan without any error"""
    query_graph, groupby_node = query_graph_with_cleaning_ops_and_groupby
    planner = FeatureExecutionPlanner(query_graph, source_info=source_info, is_online_serving=False)
    execution_plan = planner.generate_plan([groupby_node])
    groupby_node_aggregation_id = "8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8"
    assert execution_plan.feature_specs == {
        "a_2h_average": FeatureSpec(
            feature_name="a_2h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_CUSTOMER_ID_window_w7200_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
        "a_48h_average": FeatureSpec(
            feature_name="a_48h_average",
            feature_expr=quoted_identifier(
                f"_fb_internal_CUSTOMER_ID_window_w172800_avg_{groupby_node_aggregation_id}"
            ),
            feature_dtype=DBVarType.FLOAT,
        ),
    }


def test_feature_execution_planner__feature_no_entity_ids(
    query_graph_with_groupby_no_entity_ids,
    groupby_node_aggregation_id,
    source_info,
):
    """
    Test FeatureExecutionPlanner when feature node has no entity_ids
    """
    groupby_node = query_graph_with_groupby_no_entity_ids.get_node_by_name("groupby_1")
    planner = FeatureExecutionPlanner(
        query_graph_with_groupby_no_entity_ids,
        source_info=source_info,
        is_online_serving=False,
    )
    plan = planner.generate_plan([groupby_node])
    assert plan.required_entity_ids == set()


def test_feature_execution_planner__entity_relationships_context(
    complex_feature_query_graph,
    feature_node_relationships_info_business_is_parent_of_user,
    entity_lookup_step_creator,
    customer_entity_id,
    business_entity_id,
    relation_table,
    parent_serving_preparation,
    source_info,
    update_fixtures,
):
    """
    Test FeatureExecutionPlanner with EntityRelationshipsContext specified
    """
    node, graph = complex_feature_query_graph
    entity_relationships_context = EntityRelationshipsContext(
        feature_list_primary_entity_ids=[customer_entity_id],
        feature_list_serving_names=["cust_id"],
        feature_list_relationships_info=[],
        feature_node_relationships_infos=[
            feature_node_relationships_info_business_is_parent_of_user
        ],
        entity_lookup_step_creator=entity_lookup_step_creator,
    )
    parent_serving_preparation.entity_relationships_context = entity_relationships_context
    planner = FeatureExecutionPlanner(
        graph,
        source_info=source_info,
        is_online_serving=False,
        parent_serving_preparation=parent_serving_preparation,
    )
    plan = planner.generate_plan([node])

    # Check aggregation specs serving names updated correctly
    for agg_specs in plan.aggregators[
        "window"
    ].window_aggregation_spec_set.get_grouped_aggregation_specs():
        for agg_spec in agg_specs:
            assert agg_spec.keys[0] in {"cust_id", "biz_id"}
            if agg_spec.keys[0] == "cust_id":
                assert agg_spec.serving_names == ["cust_id"]
            else:
                assert agg_spec.serving_names == ["cust_id_100000000000000000000000"]

    # Check entity lookup steps recorded in plan correctly
    assert plan.feature_entity_lookup_steps == {
        "cust_id_100000000000000000000000": EntityLookupStep(
            id=ObjectId("100000000000000000000000"),
            table=relation_table.model_dump(by_alias=True),
            parent=EntityLookupInfo(
                entity_id=business_entity_id,
                key="relation_biz_id",
                serving_name="cust_id_100000000000000000000000",
            ),
            child=EntityLookupInfo(
                entity_id=customer_entity_id,
                key="relation_cust_id",
                serving_name="cust_id",
            ),
        )
    }

    # Check combined sql
    sql = (
        plan.construct_combined_sql(
            request_table_name="REQUEST_TABLE",
            point_in_time_column="POINT_IN_TIME",
            request_table_columns=["a", "b", "c"],
            prior_cte_statements=[],
            exclude_columns=None,
        )
        .get_standalone_expr()
        .sql(pretty=True)
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_combined_sql_with_relationships.sql",
        update_fixture=update_fixtures,
    )


def test_feature_query_plan(source_info, update_fixtures):
    """
    Test FeatureQueryPlan
    """
    feature_query_plan = FeatureQueryPlan(
        common_tables=[
            CommonTable(
                name="_FB_A",
                expr=select("*").from_("SOME_TABLE"),
                should_materialize=False,
            ),
            CommonTable(
                name="_FB_B",
                expr=select("*").from_("_FB_A"),
                should_materialize=True,
            ),
            CommonTable(
                name="_FB_C",
                expr=select("*").from_("_FB_B"),
                should_materialize=False,
            ),
            CommonTable(
                name="_FB_D",
                expr=select("*").from_("_FB_C"),
                should_materialize=True,
            ),
        ],
        post_aggregation_sql=select("*").from_("_FB_D"),
        feature_names=["a"],
    )
    feature_query = feature_query_plan.get_feature_query(
        table_name="MY_FEATURE_TABLE", node_names=["node_1"], source_info=source_info
    )
    queries = feature_query_to_string(feature_query)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_feature_query_plan.sql",
        update_fixtures,
    )
