"""
This module contains tests for the offline ingest query graph.
"""
import os
import textwrap
from unittest import mock

import freezegun
import pytest
from bson import json_util

from featurebyte import Entity, FeatureJobSetting, FeatureList, RequestColumn
from featurebyte.models.feature import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.transform.offline_store_ingest import AggregationNodeInfo
from tests.util.helper import (
    check_decomposed_graph_output_node_hash,
    check_on_demand_feature_code_generation,
)


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    enable_feast_integration, patched_catalog_get_create_payload
):
    """Enable feast integration & patch catalog ID for all tests in this module"""
    _ = enable_feast_integration, patched_catalog_get_create_payload
    yield


@pytest.fixture(name="latest_event_timestamp_feature")
def latest_event_timestamp_feature_fixture(
    snowflake_event_view_with_entity, feature_group_feature_job_setting
):
    """
    Fixture for a timestamp feature
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="event_timestamp",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=feature_group_feature_job_setting,
    )["latest_event_timestamp_90d"]
    return feature


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
def test_feature__ttl_and_non_ttl_components(composite_feature, test_dir, update_fixtures):
    """Test that a feature contains both ttl and non-ttl components."""
    composite_feature.save()

    # check offline ingest query graph
    feature_model = composite_feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].feature_job_setting:
        ttl_component_graph = ingest_query_graphs[0]
        non_ttl_component_graph = ingest_query_graphs[1]
    else:
        ttl_component_graph = ingest_query_graphs[1]
        non_ttl_component_graph = ingest_query_graphs[0]

    expected_feature_job_setting = FeatureJobSetting(
        blind_spot="600s", frequency="1800s", time_modulo_frequency="300s"
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

    assert non_ttl_component_graph.feature_job_setting is None
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
    patched_catalog_get_create_payload,
    non_time_based_feature,
    latest_event_timestamp_feature,
    feature_group_feature_job_setting,
    test_dir,
    update_fixtures,
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

    # check offline ingest query graph (note that the request column part should be removed)
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 2
    if ingest_query_graphs[0].feature_job_setting:
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

    assert non_ttl_component_graph.feature_job_setting is None
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
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feature_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = pd.to_datetime(inputs["__feature_V231227__part0"], utc=True)
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        feat_1 = request_col + (request_col - request_col)
        feat_2 = pd.Series(
            np.where(
                pd.isna(((feat_1 - feat).dt.seconds // 86400))
                | pd.isna(inputs["__feature_V231227__part1"]),
                np.nan,
                ((feat_1 - feat).dt.seconds // 86400)
                + inputs["__feature_V231227__part1"],
            ),
            index=((feat_1 - feat).dt.seconds // 86400).index,
        )
        # TTL handling for feature_V231227
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        feat_2[~mask] = np.nan
        df["feature_V231227"] = feat_2
        return df
    """
    assert offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


def test_feature__multiple_non_ttl_components(
    snowflake_scd_table, snowflake_dimension_table, cust_id_entity
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

    # check offline ingest query graph (note that the request column part should be removed)
    feature_model = feature.cached_model
    offline_store_info = feature_model.offline_store_info
    ingest_query_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_query_graphs) == 1
    non_ttl_component_graph = ingest_query_graphs[0]
    assert non_ttl_component_graph.feature_job_setting == FeatureJobSetting(
        blind_spot="0s", frequency="1d", time_modulo_frequency="0s"
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

    # check offline ingest query graph
    feature_model = composite_feature.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    expected = f"""
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_composite_feature_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = pd.to_datetime(
            inputs["__composite_feature_V231227__part2"], utc=True
        )
        request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        feat_1 = pd.Series(
            np.where(
                pd.isna(inputs["__composite_feature_V231227__part0"])
                | pd.isna(inputs["__composite_feature_V231227__part1"]),
                np.nan,
                inputs["__composite_feature_V231227__part0"]
                + inputs["__composite_feature_V231227__part1"],
            ),
            index=inputs["__composite_feature_V231227__part0"].index,
        )
        feat_2 = pd.Series(
            np.where(
                pd.isna(feat_1)
                | pd.isna(((request_col - feat).dt.seconds // 86400)),
                np.nan,
                feat_1 + ((request_col - feat).dt.seconds // 86400),
            ),
            index=feat_1.index,
        )
        # TTL handling for composite_feature_V231227
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        feat_2[~mask] = np.nan
        df["composite_feature_V231227"] = feat_2
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

    # check offline ingest query graph
    feature_model = feature_zscore.cached_model
    check_decomposed_graph_output_node_hash(feature_model=feature_model)
    check_on_demand_feature_code_generation(feature_model=feature_model)

    # check on-demand view code
    offline_store_info = feature_model.offline_store_info
    assert offline_store_info.odfv_info is not None
    expected = f"""
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_feature_zscore_v231227_{feature_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
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
        feat_1 = pd.Series(
            np.where(
                pd.isna(feat) | pd.isna(inputs["__feature_zscore_V231227__part2"]),
                np.nan,
                np.divide(feat, inputs["__feature_zscore_V231227__part2"]),
            ),
            index=feat.index,
        )
        # TTL handling for feature_zscore_V231227
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        feat_1[~mask] = np.nan
        df["feature_zscore_V231227"] = feat_1
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
        method="count",
        windows=["7d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["counts_7d"],
    )["counts_7d"]
    count_dict_feat2 = event_view.groupby("col_text", category="col_char").aggregate_over(
        method="count",
        windows=["14d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["counts_14d"],
    )["counts_14d"]
    feature = count_dict_feat1.cd.cosine_similarity(count_dict_feat2)
    feature.name = "feature_cosine_similarity"
    feature.save()

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
    offline_store_info = float_feature.cached_model.offline_store_info
    expected = f"""
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_sum_1d_v231227_{float_feature.cached_model.id}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        inputs["sum_1d_V231227"][~mask] = np.nan
        df["sum_1d_V231227"] = inputs["sum_1d_V231227"]
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


def test_on_demand_feature_view_code_generation__card_transaction_description_feature(test_dir):
    """Test on-demand feature view code generation for card_transaction_description feature."""
    fixture_path = os.path.join(
        test_dir, "fixtures/feature/card_txn_description_representation.json"
    )
    with open(fixture_path, "r") as file_handle:
        feature_dict = json_util.loads(file_handle.read())
        feature = FeatureModel(**feature_dict)

    # initialize offline store info
    feature.initialize_offline_store_info(entity_id_to_serving_name={})

    # check on-demand view code
    check_on_demand_feature_code_generation(feature_model=feature)

    # check the actual code
    expected = """
    CREATE FUNCTION udf_txn_cardtransactiondescription_representation_in_card_txn_count__6597d113acaf7f23202c6f74(x_1 STRING, x_2 STRING, x_3 STRING)
    RETURNS DOUBLE
    LANGUAGE PYTHON
    COMMENT ''
    AS $$
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def user_defined_function(col_1: str, col_2: str, col_3: str) -> float:
        # col_1: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part1
        # col_2: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0
        # col_3: __TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2
        feat_1 = np.nan if pd.isna(col_2) else json.loads(col_2)

        def get_relative_frequency(input_dict, key):
            if pd.isna(input_dict) or key not in input_dict:
                return np.nan
            total_count = sum(input_dict.values())
            if total_count == 0:
                return 0
            key_frequency = input_dict.get(key, 0)
            return key_frequency / total_count

        feat_2 = get_relative_frequency(feat_1, key=col_1)
        flag_1 = pd.isna(feat_2)
        feat_2 = 0 if flag_1 else feat_2
        feat_3 = np.nan if pd.isna(col_3) else json.loads(col_3)
        feat_4 = get_relative_frequency(feat_3, key=col_1)
        flag_2 = pd.isna(feat_4)
        feat_4 = 0 if flag_2 else feat_4
        feat_5 = (
            np.nan
            if pd.isna(feat_2) or pd.isna(feat_4)
            else np.divide(feat_2, feat_4)
        )
        return feat_5

    return user_defined_function(x_1, x_2, x_3)
    $$
    """
    assert feature.offline_store_info.udf_info.codes.strip() == textwrap.dedent(expected).strip()

    expected = """
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def odfv_txn_cardtransactiondescription_representation_in_card_txn_count__6597d113acaf7f23202c6f74(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        feat = inputs[
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part0"
        ].apply(lambda x: np.nan if pd.isna(x) else json.loads(x))

        def get_relative_frequency(input_dict, key):
            if pd.isna(input_dict) or key not in input_dict:
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
        mask = feat_1.isnull()
        feat_1[mask] = 0
        feat_2 = inputs[
            "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part2"
        ].apply(lambda x: np.nan if pd.isna(x) else json.loads(x))
        feat_3 = feat_2.combine(
            inputs[
                "__TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105__part1"
            ],
            lambda dct, key: get_relative_frequency(dct, key=key),
        )
        mask_1 = feat_3.isnull()
        feat_3[mask_1] = 0
        feat_4 = pd.Series(
            np.where(
                pd.isna(feat_1) | pd.isna(feat_3), np.nan, np.divide(feat_1, feat_3)
            ),
            index=feat_1.index,
        )
        # TTL handling for TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=172800)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask_2 = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        feat_4[~mask_2] = np.nan
        df[
            "TXN_CardTransactionDescription_Representation_in_CARD_Txn_Count_90d_V240105"
        ] = feat_4
        return df
    """
    assert feature.offline_store_info.odfv_info.codes.strip() == textwrap.dedent(expected).strip()


@freezegun.freeze_time("2024-01-03")
def test_databricks_specs(
    float_feature,
    non_time_based_feature,
    composite_feature,
    latest_event_timestamp_feature,
    catalog_id,
):
    """Test databricks specs"""
    req_col_feature = (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day
    req_col_feature.name = "req_col_feature"
    features = [float_feature, non_time_based_feature, composite_feature, req_col_feature]
    for feature in features:
        feature.save()

    feature_list = FeatureList(features, name="feature_list")
    with mock.patch("featurebyte.models.feature_list.SourceType") as mock_source_type:
        # mock source type to be snowflake to trigger databricks specs generation
        mock_source_type.DATABRICKS = "snowflake"
        feature_list.save()

    store_info = feature_list.cached_model.store_info
    expected = """
    # auto-generated by FeatureByte (based-on databricks-feature-store 0.16.3)
    # Import necessary modules for feature engineering and machine learning
    from databricks.feature_engineering import FeatureEngineeringClient
    from databricks.feature_engineering import FeatureFunction, FeatureLookup
    from pyspark.sql.types import LongType, StructField, StructType, TimestampType
    from sklearn import linear_model
    import featurebyte as fb
    import mlflow

    # Initialize the Feature Engineering client to interact with Databricks Feature Store
    fe = FeatureEngineeringClient()

    # Timestamp column name used to retrieve the latest feature values
    timestamp_lookup_key = "POINT_IN_TIME"

    # Define the features for the model
    # FeatureLookup is used to specify how to retrieve features from the feature store
    # Each FeatureLookup or FeatureFunction object defines a set of features to be included
    features = [
        FeatureLookup(
            table_name="fb_entity_cust_id_fjs_1800_300_600_ttl_[CATALOG_ID]",
            lookup_key=["cust_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=[
                "sum_1d_V240103",
                "__feature_V240103__part0",
                "__req_col_feature_V240103__part0",
            ],
            rename_outputs={"sum_1d_V240103": "sum_1d"},
        ),
        FeatureLookup(
            table_name="fb_entity_transaction_id_fjs_86400_0_0_[CATALOG_ID]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=None,
            lookback_window=None,
            feature_names=["non_time_time_sum_amount_feature_V240103"],
            rename_outputs={
                "non_time_time_sum_amount_feature_V240103": "non_time_time_sum_amount_feature"
            },
        ),
        FeatureLookup(
            table_name="fb_entity_transaction_id_[CATALOG_ID]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=None,
            lookback_window=None,
            feature_names=["__feature_V240103__part1"],
            rename_outputs={},
        ),
        FeatureFunction(
            udf_name="udf_feature_v240103_[FEATURE_ID1]",
            input_bindings={
                "col_1": "__feature_V240103__part1",
                "col_2": "__feature_V240103__part0",
            },
            output_name="feature",
        ),
        FeatureFunction(
            udf_name="udf_req_col_feature_v240103_[FEATURE_ID2]",
            input_bindings={
                "col_1": "__req_col_feature_V240103__part0",
                "request_col_1": "POINT_IN_TIME",
            },
            output_name="req_col_feature",
        ),
    ]

    # List of columns to exclude from the training set
    # Users should consider including request columns and primary entity columns here
    # This is important if these columns are not features but are only needed for lookup purposes
    exclude_columns = [
        "__feature_V240103__part0",
        "__feature_V240103__part1",
        "__req_col_feature_V240103__part0",
    ]

    # Prepare the dataset for log model
    # 'features' is a list of feature lookups to be included in the training set
    # 'exclude_columns' is a list of columns to be excluded from the training set
    target_column = "[TARGET_COLUMN]"
    schema = StructType(
        [
            StructField("transaction_id", LongType()),
            StructField("cust_id", LongType()),
            StructField("POINT_IN_TIME", TimestampType()),
        ]
    )
    log_model_dataset = fe.create_training_set(
        df=spark.createDataFrame([], schema),
        feature_lookups=features,
        label=target_column,
        exclude_columns=exclude_columns,
    )

    # Retrieve the training dataframe through FeatureByte's compute_historical_features API
    # Observation table should include the primary entity columns, the request columns, and the target column
    catalog = fb.activate_and_get_catalog("[CATALOG_NAME]")
    feature_list = catalog.get_feature_list("[FEATURE_LIST_NAME]")
    observation_table = catalog.get_observation_table("[OBSERVATION_TABLE_NAME]")
    training_df = feature_list.compute_historical_features(
        observation_set=observation_table.to_pandas(),
    )

    # Separate the features (X_train) and the target variable (y_train) for model training
    X_train = training_df.drop([target_column], axis=1)
    y_train = training_df[target_column]

    # Create and train the linear regression model using the training data
    model = linear_model.LinearRegression().fit(X_train, y_train)

    # Log the model and register it to the unity catalog
    mlflow.set_registry_uri("databricks-uc")

    fe.log_model(
        model=model,
        artifact_path="main.default.model",
        flavor=mlflow.sklearn,
        training_set=log_model_dataset,
        registered_model_name="main.default.recommender_model",
    )
    """
    replace_pairs = [
        ("[CATALOG_ID]", str(catalog_id)),
        ("[FEATURE_ID1]", str(composite_feature.cached_model.id)),
        ("[FEATURE_ID2]", str(req_col_feature.cached_model.id)),
    ]
    for replace_pair in replace_pairs:
        expected = expected.replace(*replace_pair)
    assert store_info.feature_specs_definition.strip() == textwrap.dedent(expected).strip()
