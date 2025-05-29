"""
This module contains tests for the offline ingest query graph.
"""

import os
import textwrap

import freezegun
import pytest
from bson import json_util

from featurebyte import Entity, FeatureJobSetting, RequestColumn
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.transform.offline_store_ingest import AggregationNodeInfo
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.catalog import CatalogCreate
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_code_generation,
    deploy_features_through_api,
)


@pytest.fixture(name="default_feature_job_setting")
def default_feature_job_setting_fixture():
    """Fixture for default feature job setting"""
    return FeatureJobSetting(blind_spot="0s", period="1d", offset="0s")


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    patched_catalog_get_create_payload,
    mock_deployment_flow,
):
    """Enable feast integration & patch catalog ID for all tests in this module"""
    _ = patched_catalog_get_create_payload, mock_deployment_flow
    yield


@pytest.fixture(name="entity_id_to_serving_name")
def entity_id_to_serving_name_fixture(cust_id_entity, transaction_entity):
    """Fixture for entity id to serving name"""
    return {
        cust_id_entity.id: cust_id_entity.serving_names[0],
        transaction_entity.id: transaction_entity.serving_names[0],
    }


def check_ingest_query_graph(ingest_query_graph):
    """Check the ingest query graph."""
    graph = ingest_query_graph.graph
    for aggregation_node_info in ingest_query_graph.aggregation_nodes_info:
        agg_node = graph.get_node_by_name(aggregation_node_info.node_name)
        assert agg_node.type == aggregation_node_info.node_type
        input_node_names = graph.get_input_node_names(agg_node)
        assert len(input_node_names) == 1
        assert input_node_names[0] == aggregation_node_info.input_node_name

    # check consistency of entity info
    assert len(ingest_query_graph.primary_entity_ids) == len(
        ingest_query_graph.primary_entity_dtypes
    )


@pytest.fixture(name="composite_feature")
def composite_feature_fixture(float_feature, non_time_based_feature):
    """Fixture for a composite feature"""
    ttl_component = 2 * (float_feature + 100)
    non_ttl_component = 3 - (non_time_based_feature + 100)
    feature = ttl_component + non_ttl_component
    feature.name = "feature"
    return feature


@freezegun.freeze_time("2023-12-29")
def test_feature__ttl_and_non_ttl_components(
    ttl_non_ttl_composite_feature, test_dir, update_fixtures, default_feature_job_setting
):
    """Test that a feature contains both ttl and non-ttl components."""
    ttl_non_ttl_composite_feature.save()
    deploy_features_through_api([ttl_non_ttl_composite_feature])

    # check offline ingest query graph
    feature_model = ttl_non_ttl_composite_feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].has_ttl:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    expected_feature_job_setting = FeatureJobSetting(
        blind_spot="600s", period="1800s", offset="300s"
    )
    assert ttl_component_graph.feature_job_setting == expected_feature_job_setting
    assert ttl_component_graph.node_name == "mul_1"
    assert ttl_component_graph.has_ttl is True
    assert ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.GROUPBY, node_name="groupby_1", input_node_name="graph_1"
        ),
    ]
    check_ingest_query_graph(ttl_component_graph)

    assert non_ttl_component_graph.feature_job_setting == default_feature_job_setting
    assert non_ttl_component_graph.node_name == "sub_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.ITEM_GROUPBY, node_name="item_groupby_1", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    sql_fixture_path = os.path.join(test_dir, "fixtures/on_demand_function/ttl_and_non_ttl.sql")
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(
        feature_model=feature_model,
        sql_fixture_path=sql_fixture_path,
        update_fixtures=update_fixtures,
    )


@freezegun.freeze_time("2023-12-27")
def test_feature__request_column_ttl_and_non_ttl_components(
    non_time_based_feature,
    latest_event_timestamp_feature,
    feature_group_feature_job_setting,
    test_dir,
    update_fixtures,
    default_feature_job_setting,
):
    """Test that a feature contains request column, ttl and non-ttl components."""
    request_and_ttl_component = (
        # request component part
        RequestColumn.point_in_time()
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time())
        # ttl component part
        - latest_event_timestamp_feature
    ).dt.day
    non_ttl_component = non_time_based_feature
    feature = request_and_ttl_component + non_ttl_component
    feature.name = "feature"
    feature.save()
    deploy_features_through_api([feature])

    # check offline ingest query graph (note that the request column part should be removed)
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].has_ttl:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    assert ttl_component_graph.feature_job_setting == feature_group_feature_job_setting
    assert ttl_component_graph.node_name == "project_1"
    assert ttl_component_graph.has_ttl is True
    assert ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.GROUPBY, node_name="groupby_1", input_node_name="graph_1"
        ),
    ]
    check_ingest_query_graph(ttl_component_graph)

    assert non_ttl_component_graph.feature_job_setting == default_feature_job_setting
    assert non_ttl_component_graph.node_name == "project_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.ITEM_GROUPBY, node_name="item_groupby_1", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    sql_fixture_path = os.path.join(
        test_dir, "fixtures/on_demand_function/req_col_ttl_and_non_ttl.sql"
    )
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(
        feature_model=feature_model,
        sql_fixture_path=sql_fixture_path,
        update_fixtures=update_fixtures,
    )

    # check on-demand view code
    assert offline_store_info.odfv_info is not None
    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feature_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        feat = pd.to_datetime(request_col, utc=True) - pd.to_datetime(
            request_col, utc=True
        )
        feat_1 = pd.to_datetime(request_col, utc=True) + pd.to_timedelta(feat)

        # TTL handling for __feature_V231227__part0 column
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feat_ts = pd.to_datetime(
            inputs["__feature_V231227__part0__ts"], utc=True, unit="s"
        )
        mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
        inputs.loc[~mask, "__feature_V231227__part0"] = np.nan
        feat_2 = pd.to_datetime(inputs["__feature_V231227__part0"], utc=True)
        feat_3 = pd.to_datetime(feat_1, utc=True) - pd.to_datetime(feat_2, utc=True)
        feat_4 = pd.to_timedelta(feat_3).dt.total_seconds() / 86400
        feat_5 = pd.Series(
            np.where(
                pd.isna(feat_4) | pd.isna(inputs["__feature_V231227__part1"]),
                np.nan,
                feat_4 + inputs["__feature_V231227__part1"],
            ),
            index=feat_4.index,
        )
        df["feature_V231227"] = feat_5
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


def test_feature__multiple_non_ttl_components(
    snowflake_scd_table, snowflake_dimension_table, cust_id_entity, patch_initialize_entity_dtype
):
    """Test that a feature contains multiple non-ttl components."""
    snowflake_scd_table["col_text"].as_entity(cust_id_entity.name)
    scd_view = snowflake_scd_table.get_view()
    lookup_feature = scd_view["col_float"].as_feature("FloatFeature", offset="7d")

    snowflake_dimension_table["col_int"].as_entity(cust_id_entity.name)
    dimension_view = snowflake_dimension_table.get_view()
    feature_a = dimension_view["col_float"].as_feature("FloatFeatureDimensionView")
    feature = lookup_feature + feature_a
    feature.name = "feature"
    feature.save()
    deploy_features_through_api([feature])

    # check offline ingest query graph (note that the request column part should be removed)
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 1
    non_ttl_component_graph = ingest_query_graphs[0]
    assert non_ttl_component_graph.feature_job_setting == FeatureJobSetting(
        blind_spot="0s", period="1d", offset="0s"
    )
    assert non_ttl_component_graph.node_name == "alias_1"
    assert non_ttl_component_graph.has_ttl is False
    assert non_ttl_component_graph.aggregation_nodes_info == [
        AggregationNodeInfo(
            node_type=NodeType.LOOKUP, node_name="lookup_1", input_node_name="graph_1"
        ),
        AggregationNodeInfo(
            node_type=NodeType.LOOKUP, node_name="lookup_2", input_node_name="graph_2"
        ),
    ]
    check_ingest_query_graph(non_ttl_component_graph)

    # check consistency of decomposed graph
    check_decomposed_graph_output_node_hash(feature_model=feature.cached_model)

    # check on-demand view code
    assert offline_store_info.is_decomposed is False
    assert offline_store_info.odfv_info is None


@freezegun.freeze_time("2023-12-27")
def test_feature__ttl_item_aggregate_request_column(
    float_feature, non_time_based_feature, latest_event_timestamp_feature
):
    """Test that a feature contains ttl item aggregate and request column components."""
    request_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    composite_feature = float_feature + non_time_based_feature + request_feature
    composite_feature.name = "composite_feature"
    composite_feature.save()
    deploy_features_through_api([composite_feature])

    # check offline ingest query graph
    feature_model = composite_feature.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_composite_feature_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)

        # TTL handling for __composite_feature_V231227__part0 column
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feat_ts = pd.to_datetime(
            inputs["__composite_feature_V231227__part0__ts"], utc=True, unit="s"
        )
        mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
        inputs.loc[~mask, "__composite_feature_V231227__part0"] = np.nan
        feat = pd.to_datetime(
            inputs["__composite_feature_V231227__part0"], utc=True
        )
        feat_1 = pd.to_datetime(request_col, utc=True) - pd.to_datetime(
            feat, utc=True
        )
        feat_2 = pd.to_timedelta(feat_1).dt.total_seconds() / 86400

        # TTL handling for __composite_feature_V231227__part1 column
        request_time_1 = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff_1 = request_time_1 - pd.Timedelta(seconds=3600)
        feat_ts_1 = pd.to_datetime(
            inputs["__composite_feature_V231227__part1__ts"], utc=True, unit="s"
        )
        mask_1 = (feat_ts_1 >= cutoff_1) & (feat_ts_1 <= request_time_1)
        inputs.loc[~mask_1, "__composite_feature_V231227__part1"] = np.nan
        feat_3 = pd.Series(
            np.where(
                pd.isna(inputs["__composite_feature_V231227__part1"])
                | pd.isna(inputs["__composite_feature_V231227__part2"]),
                np.nan,
                inputs["__composite_feature_V231227__part1"]
                + inputs["__composite_feature_V231227__part2"],
            ),
            index=inputs["__composite_feature_V231227__part1"].index,
        )
        feat_4 = pd.Series(
            np.where(pd.isna(feat_3) | pd.isna(feat_2), np.nan, feat_3 + feat_2),
            index=feat_3.index,
        )
        df["composite_feature_V231227"] = feat_4
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


@freezegun.freeze_time("2023-12-27")
def test_feature__input_has_mixed_ingest_graph_node_flags(
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
):
    """Test that a feature with mixed ingest graph node flags input nodes."""
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    event_view = snowflake_event_table_with_entity.get_view()
    grouped_event_view = event_view.groupby("cust_id")
    feature_raw = event_view.as_features(column_names=["col_float"], feature_names=["col_float"])[
        "col_float"
    ]
    feature_avg = grouped_event_view.aggregate_over(
        value_column="col_float",
        method="avg",
        windows=["90d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["avg_col_float_90d"],
    )["avg_col_float_90d"]
    feature_std = grouped_event_view.aggregate_over(
        value_column="col_float",
        method="std",
        windows=["90d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["std_col_float_90d"],
    )["std_col_float_90d"]
    feature_zscore = (feature_raw - feature_avg) / feature_std
    feature_zscore.name = "feature_zscore"
    feature_zscore.save()
    deploy_features_through_api([feature_zscore])

    # check offline ingest query graph
    feature_model = feature_zscore.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    assert offline_store_info.odfv_info is not None
    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feature_zscore_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()

        # TTL handling for __feature_zscore_V231227__part1 column
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feat_ts = pd.to_datetime(
            inputs["__feature_zscore_V231227__part1__ts"], utc=True, unit="s"
        )
        mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
        inputs.loc[~mask, "__feature_zscore_V231227__part1"] = np.nan
        feat = pd.Series(
            np.where(
                pd.isna(inputs["__feature_zscore_V231227__part0"])
                | pd.isna(inputs["__feature_zscore_V231227__part1"]),
                np.nan,
                inputs["__feature_zscore_V231227__part0"]
                - inputs["__feature_zscore_V231227__part1"],
            ),
            index=inputs["__feature_zscore_V231227__part0"].index,
        )

        # TTL handling for __feature_zscore_V231227__part2 column
        request_time_1 = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff_1 = request_time_1 - pd.Timedelta(seconds=3600)
        feat_ts_1 = pd.to_datetime(
            inputs["__feature_zscore_V231227__part2__ts"], utc=True, unit="s"
        )
        mask_1 = (feat_ts_1 >= cutoff_1) & (feat_ts_1 <= request_time_1)
        inputs.loc[~mask_1, "__feature_zscore_V231227__part2"] = np.nan
        feat_1 = pd.Series(
            np.where(
                pd.isna(feat) | pd.isna(inputs["__feature_zscore_V231227__part2"]),
                np.nan,
                np.divide(feat, inputs["__feature_zscore_V231227__part2"]),
            ),
            index=feat.index,
        )
        df["feature_zscore_V231227"] = feat_1
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()

    feat = feature_raw[feature_avg > 0]
    feat.name = "feature"
    feat.save()
    deploy_features_through_api([feat])
    offline_store_info = feat.cached_model.offline_store_info
    assert offline_store_info.is_decomposed is True

    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feature_v231227_{feat.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()

        # TTL handling for __feature_V231227__part1 column
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feat_ts = pd.to_datetime(
            inputs["__feature_V231227__part1__ts"], utc=True, unit="s"
        )
        mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
        inputs.loc[~mask, "__feature_V231227__part1"] = np.nan
        feat = inputs["__feature_V231227__part0"][
            inputs["__feature_V231227__part1"]
        ].reindex(index=inputs["__feature_V231227__part0"].index)
        df["feature_V231227"] = feat
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


def test_feature__composite_count_dict(
    snowflake_event_table_with_entity, feature_group_feature_job_setting
):
    """Test that a feature with composite count dict feature."""
    another_entity = Entity.create(name="another_entity", serving_names=["another_entity"])
    snowflake_event_table_with_entity.col_text.as_entity(another_entity.name)
    event_view = snowflake_event_table_with_entity.get_view()
    count_dict_feat1 = event_view.groupby("cust_id", category="col_char").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["counts_7d"],
    )["counts_7d"]
    count_dict_feat2 = event_view.groupby("col_text", category="col_char").aggregate_over(
        value_column=None,
        method="count",
        windows=["14d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["counts_14d"],
    )["counts_14d"]
    feature = count_dict_feat1.cd.cosine_similarity(count_dict_feat2)
    feature.name = "feature_cosine_similarity"
    feature.save()
    deploy_features_through_api([feature])

    # check offline ingest query graph
    feature_model = feature.cached_model
    assert feature_model.offline_store_info.is_decomposed is True
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(feature_model=feature_model)


def test_feature__input_has_ingest_query_graph_node(test_dir):
    """
    Test a complex feature when there exist a node fulfills the split condition except all the input nodes
    have already contained ingest query graph nodes.
    """
    fixture_path = os.path.join(
        test_dir,
        "fixtures/graph/TXN_IsWeekend_Representation_in_CARD_Cash_advance_Txn_Amount_90d.json",
    )
    with open(fixture_path, "r") as file_handle:
        feature_dict = json_util.loads(file_handle.read())

    feature_model = FeatureModel(**feature_dict)
    check_decomposed_graph_output_node_hash(feature_model=feature_model)


@freezegun.freeze_time("2023-12-27")
def test_feature__with_ttl_handling(float_feature):
    """Test a feature with ttl handling."""
    float_feature.save()
    deploy_features_through_api([float_feature])
    offline_store_info = float_feature.cached_model.offline_store_info
    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_sum_1d_v231227_{float_feature.cached_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        # Time-to-live (TTL) handling to clean up expired data
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(
            inputs["sum_1d_V231227__ts"], unit="s", utc=True
        )
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        inputs.loc[~mask, "sum_1d_V231227"] = np.nan
        df["sum_1d_V231227"] = inputs["sum_1d_V231227"]
        df.fillna(np.nan, inplace=True)
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


def test_feature_entity_dtypes(
    snowflake_event_table, cust_id_entity, transaction_entity, arbitrary_default_feature_job_setting
):
    """Test that entity dtypes are correctly set."""
    snowflake_event_table.col_int.as_entity(cust_id_entity.name)
    snowflake_event_table.col_text.as_entity(transaction_entity.name)
    snowflake_event_table.update_default_feature_job_setting(
        feature_job_setting=arbitrary_default_feature_job_setting,
    )
    event_view = snowflake_event_table.get_view()

    feat_sum1 = event_view.groupby("col_int").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["24h"],
        feature_names=["sum_a_24h"],
    )["sum_a_24h"]

    feat_sum2 = event_view.groupby("col_text").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["24h"],
        feature_names=["sum_b_24h"],
    )["sum_b_24h"]

    feat = feat_sum1 + feat_sum2
    feat.name = "feature"
    feat.save()
    deploy_features_through_api([feat])

    # check the entity dtypes are correctly set
    expected_entity_id_to_dtype = {
        cust_id_entity.id: snowflake_event_table.col_int.info.dtype,
        transaction_entity.id: snowflake_event_table.col_text.info.dtype,
    }
    assert feat.cached_model.entity_dtypes == [
        expected_entity_id_to_dtype[entity_id] for entity_id in feat.entity_ids
    ]

    # check ingest query graph
    offline_store_info = feat.cached_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 1
    assert ingest_query_graphs[0].primary_entity_ids == [cust_id_entity.id]
    assert ingest_query_graphs[0].primary_entity_dtypes == [
        expected_entity_id_to_dtype[cust_id_entity.id]
    ]


@pytest.mark.asyncio
async def test_on_demand_feature_view_code_generation__card_transaction_description_feature(
    test_dir, persistent, user, snowflake_feature_store
):
    """Test on-demand feature view code generation for card_transaction_description feature."""
    fixture_path = os.path.join(
        test_dir, "fixtures/feature/card_txn_description_representation.json"
    )
    with open(fixture_path, "r") as file_handle:
        feature_dict = json_util.loads(file_handle.read())
        feature = FeatureModel(**feature_dict)

    # create catalog document
    catalog_id = feature.catalog_id
    app_container = LazyAppContainer(
        app_container_config=app_container_config,
        instance_map={
            "user": user,
            "persistent": persistent,
            "catalog_id": catalog_id,
        },
    )
    await app_container.catalog_service.create_document(
        data=CatalogCreate(
            _id=catalog_id,
            name="test_catalog",
            default_feature_store_ids=[snowflake_feature_store.id],
        )
    )

    # initialize offline store info
    service = app_container.offline_store_info_initialization_service
    offline_store_info = await service.initialize_offline_store_info(
        feature=feature,
        entity_id_to_serving_name={entity_id: str(entity_id) for entity_id in feature.entity_ids},
        table_name_prefix="cat1",
    )

    # check on-demand view code
    feature.internal_offline_store_info = offline_store_info.model_dump(by_alias=True)
    check_on_demand_feature_code_generation(feature_model=feature)

    # check the actual code
    expected = """
    CREATE FUNCTION udf_txn_cardtransactiondescription_representation_in_card_txn_count__6597d113acaf7f23202c6f74(x_1 STRING, x_2 MAP<STRING, DOUBLE>, x_3 MAP<STRING, DOUBLE>)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    COMMENT ''
    AS $$
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def user_defined_function(
        col_1: str, col_2: dict[str, float], col_3: dict[str, float]
    ) -> float:
        # col_1: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part1
        # col_2: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2
        # col_3: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0
        feat_1 = np.nan if pd.isna(col_2) else col_2

        def get_relative_frequency(input_dict, key):
            if pd.isna(input_dict) or pd.isna(key):
                return np.nan
            key = (
                str(int(key))
                if isinstance(key, (int, float, np.integer, np.floating))
                else key
            )
            if key not in input_dict:
                return np.nan
            total_count = sum(input_dict.values())
            if total_count == 0:
                return 0
            key_frequency = input_dict.get(key, 0)
            return key_frequency / total_count

        feat_2 = get_relative_frequency(feat_1, key=col_1)
        flag_1 = pd.isna(feat_2)
        feat_3 = 0 if flag_1 else feat_2
        feat_4 = np.nan if pd.isna(col_3) else col_3
        feat_5 = get_relative_frequency(feat_4, key=col_1)
        flag_2 = pd.isna(feat_5)
        feat_6 = 0 if flag_2 else feat_5
        feat_7 = (
            np.nan
            if pd.isna(feat_6) or pd.isna(feat_3)
            else np.divide(feat_6, feat_3)
        )
        return feat_7

    output = user_defined_function(x_1, x_2, x_3)
    return None if pd.isnull(output) else output
    $$
    """
    assert offline_store_info.udf_info.codes.strip() == textwrap.dedent(expected).strip()

    expected = """
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_txn_cardtransactiondescription_representation_in_card_txn_count__6597d113acaf7f23202c6f74(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()

        # TTL handling for __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2 column
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=172800)
        feat_ts = pd.to_datetime(
            inputs[
                "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2__ts"
            ],
            utc=True,
            unit="s",
        )
        mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
        inputs.loc[
            ~mask,
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2",
        ] = np.nan
        feat = inputs[
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2"
        ].apply(lambda x: np.nan if pd.isna(x) else json.loads(x))

        def get_relative_frequency(input_dict, key):
            if pd.isna(input_dict) or pd.isna(key):
                return np.nan
            key = (
                str(int(key))
                if isinstance(key, (int, float, np.integer, np.floating))
                else key
            )
            if key not in input_dict:
                return np.nan
            total_count = sum(input_dict.values())
            if total_count == 0:
                return 0
            key_frequency = input_dict.get(key, 0)
            return key_frequency / total_count

        feat_1 = feat.combine(
            inputs[
                "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part1"
            ],
            lambda dct, key: get_relative_frequency(dct, key=key),
        )
        mask_1 = feat_1.isnull()
        feat_1[mask_1] = 0

        # TTL handling for __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0 column
        request_time_1 = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff_1 = request_time_1 - pd.Timedelta(seconds=172800)
        feat_ts_1 = pd.to_datetime(
            inputs[
                "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0__ts"
            ],
            utc=True,
            unit="s",
        )
        mask_2 = (feat_ts_1 >= cutoff_1) & (feat_ts_1 <= request_time_1)
        inputs.loc[
            ~mask_2,
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0",
        ] = np.nan
        feat_2 = inputs[
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0"
        ].apply(lambda x: np.nan if pd.isna(x) else json.loads(x))
        feat_3 = feat_2.combine(
            inputs[
                "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part1"
            ],
            lambda dct, key: get_relative_frequency(dct, key=key),
        )
        mask_3 = feat_3.isnull()
        feat_3[mask_3] = 0
        feat_4 = pd.Series(
            np.where(
                pd.isna(feat_3) | pd.isna(feat_1), np.nan, np.divide(feat_3, feat_1)
            ),
            index=feat_3.index,
        )
        df[
            "TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105"
        ] = feat_4
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


@freezegun.freeze_time("2025-01-30")
def test_time_series_feature_offline_ingest_query_graph(ts_window_aggregate_feature):
    """Test offline ingest query graph for time series feature."""
    # save features first
    ts_window_aggregate_feature.save()
    request_and_ttl_component = (
        RequestColumn.point_in_time()
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time())
    ).dt.day
    complex_feature = ts_window_aggregate_feature + request_and_ttl_component
    complex_feature.name = "feature"
    complex_feature.save()

    # deploy features
    deploy_features_through_api([ts_window_aggregate_feature, complex_feature])

    # check offline ingest query graph (time series window aggregate feature)
    offline_store_info = ts_window_aggregate_feature.cached_model.offline_store_info
    assert offline_store_info.is_decomposed is False
    version_name = ts_window_aggregate_feature.cached_model.versioned_name

    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp
    import croniter
    import pytz
    from zoneinfo import ZoneInfo


    def odfv_{version_name.lower()}_{str(ts_window_aggregate_feature.id).lower()}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()

        # Time-to-live (TTL) handling to clean up expired data
        cron = croniter.croniter("0 8 1 * *")
        prev_time = cron.timestamp_to_datetime(cron.get_prev())
        prev_time = prev_time.replace(tzinfo=ZoneInfo("Etc/UTC")).astimezone(
            pytz.utc
        )
        feature_timestamp = pd.to_datetime(
            inputs["{version_name}__ts"], unit="s", utc=True
        )
        mask = feature_timestamp <= prev_time
        inputs.loc[mask, "{version_name}"] = np.nan
        df["{version_name}"] = inputs["{version_name}"]
        df.fillna(np.nan, inplace=True)

        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()

    # check composite feature
    offline_store_info = complex_feature.cached_model.offline_store_info
    assert offline_store_info.is_decomposed is True
    version_name = complex_feature.cached_model.versioned_name

    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp
    import croniter
    import pytz
    from zoneinfo import ZoneInfo


    def odfv_{version_name.lower()}_{str(complex_feature.id).lower()}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        feat = pd.to_datetime(request_col, utc=True) - pd.to_datetime(
            request_col, utc=True
        )
        feat_1 = pd.to_datetime(request_col, utc=True) + pd.to_timedelta(feat)

        # TTL handling for __{version_name}__part0 column with cron expression 0 8 1 * *
        cron = croniter.croniter("0 8 1 * *")
        prev_time = cron.timestamp_to_datetime(cron.get_prev())
        prev_time = prev_time.replace(tzinfo=ZoneInfo("Etc/UTC")).astimezone(
            pytz.utc
        )
        feat_ts = pd.to_datetime(
            inputs["__{version_name}__part0__ts"], utc=True, unit="s"
        )
        mask = feat_ts <= prev_time
        inputs.loc[mask, "__{version_name}__part0"] = np.nan
        feat_2 = pd.Series(
            np.where(
                pd.isna(inputs["__{version_name}__part0"])
                | pd.isna((pd.to_datetime(feat_1, utc=True).dt.day)),
                np.nan,
                inputs["__{version_name}__part0"]
                + (pd.to_datetime(feat_1, utc=True).dt.day),
            ),
            index=inputs["__{version_name}__part0"].index,
        )
        df["{version_name}"] = feat_2
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()
