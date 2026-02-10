"""
Test the count dictionary nodes in the on-demand view code generation.
"""

import numpy as np
import pandas as pd
import pytest

from featurebyte.query_graph.node.count_dict import (
    CosineSimilarityNode,
    CountDictTransformNode,
    DictionaryKeysNode,
    GetRankFromDictionaryNode,
    GetRelativeFrequencyFromDictionaryNode,
    GetValueFromDictionaryNode,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    VariableNameGenerator,
    VariableNameStr,
)
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results

NODE_PARAMS = {"name": "node_name"}


@pytest.fixture(name="count_dict_feature1")
def fixture_count_dict_feature1():
    """Fixture for the count dict feature"""
    return pd.Series([
        None,
        {"a": 1},
        {},
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 2, "c": 3, "__MISSING__": 4},
        {"0": 0.5, "1": 1, "2": 2, "3": 3},
        {"0": 1, "1": 2, "2": 3, "3": 4},
    ])


@pytest.fixture(name="count_dict_feature2")
def fixture_count_dict_feature2():
    """Fixture for the count dict feature"""
    return pd.Series([
        {"a": 1},
        None,
        {"b": 1},
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 2},
        {"0": 1, "1": 1.5, "2": 2.5},
        {"0": 1.5, "1": 1, "2": 3},
    ])


@pytest.fixture(name="rank_feat")
def fixture_rank_feat():
    """Fixture for testing get rank"""
    return pd.Series([
        None,
        {"a": 1},
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 1, "c": 1},
        {"a": 1, "b": 2},
        {"a": 1, "b": 2},
        {"0": 0.5, "1": 1, "2": 2},
        {"0": 1, "1": 2, "2": 3},
    ])


@pytest.fixture(name="item_feature")
def fixture_item_feature():
    """Fixture for the item feature"""
    return pd.Series([
        "a",
        None,
        "a",
        "b",
        "d",
        "0",
        None,
    ])


@pytest.fixture(name="rank_key_feat")
def fixture_rank_key_feat():
    """Fixture for key for get rank"""
    return pd.Series([
        "a",
        None,
        "non_existing_key",
        "a",
        "b",
        "a",
        "b",
        "1",
        None,
    ])


@pytest.mark.parametrize(
    "node_params, expected_values",
    [
        (
            {"parameters": {"transform_type": "entropy"}},
            pd.Series([np.nan, 0, 0, 1.098612, 1.279854, 1.204793, 1.279854]),
        ),
        (
            {"parameters": {"transform_type": "most_frequent"}},
            pd.Series([np.nan, "a", np.nan, "a", "__MISSING__", "3", "3"]),
        ),
        (
            {"parameters": {"transform_type": "key_with_highest_value"}},
            pd.Series([np.nan, "a", np.nan, "a", "__MISSING__", "3", "3"]),
        ),
        (
            {"parameters": {"transform_type": "key_with_lowest_value"}},
            pd.Series([np.nan, "a", np.nan, "a", "a", "0", "0"]),
        ),
        (
            {"parameters": {"transform_type": "unique_count", "include_missing": True}},
            pd.Series([np.nan, 1, 0, 3, 4, 4, 4]),
        ),
        (
            {"parameters": {"transform_type": "unique_count", "include_missing": False}},
            pd.Series([np.nan, 1, 0, 3, 3, 4, 4]),
        ),
        (
            {"parameters": {"transform_type": "normalize"}},
            pd.Series([
                np.nan,  # None -> nan (null input)
                {"a": 1.0},  # {"a": 1} -> {"a": 1.0}
                {},  # {} -> {} (empty dict)
                {"a": 1 / 3, "b": 1 / 3, "c": 1 / 3},  # sum=3, each value/3
                {"a": 0.1, "b": 0.2, "c": 0.3, "__MISSING__": 0.4},  # sum=10
                {"0": 0.5 / 6.5, "1": 1 / 6.5, "2": 2 / 6.5, "3": 3 / 6.5},  # sum=6.5
                {"0": 0.1, "1": 0.2, "2": 0.3, "3": 0.4},  # sum=10
            ]),
        ),
    ],
)
def test_derive_on_demand_view_code__count_dict_transform(
    node_params,
    odfv_config,
    udf_config,
    expected_values,
    count_dict_feature1,
    node_code_gen_output_factory,
):
    """Test derive_on_demand_view_code"""
    node = CountDictTransformNode(**NODE_PARAMS, **node_params)
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": count_dict_feature1},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=expected_values,
    )


def test_derive_on_demand_view_code__cosine_similarity(
    count_dict_feature1, count_dict_feature2, odfv_config, udf_config, node_code_gen_output_factory
):
    """Test derive_on_demand_view_code"""
    node = CosineSimilarityNode(**NODE_PARAMS)
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": count_dict_feature1, "feat2": count_dict_feature2},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series([np.nan, np.nan, 0.0, 1.0, 0.408248, 0.601629, 0.652051]),
    )


def test_derive_on_demand_view_code__dictionary_keys(
    count_dict_feature1, odfv_config, udf_config, node_code_gen_output_factory
):
    """Test derive_on_demand_view_code"""
    node = DictionaryKeysNode(**NODE_PARAMS)
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": count_dict_feature1},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series([
            np.nan,
            ["a"],
            [],
            ["a", "b", "c"],
            ["a", "b", "c", "__MISSING__"],
            ["0", "1", "2", "3"],
            ["0", "1", "2", "3"],
        ]),
    )


def test_derive_on_demand_view_code__dictionary_get_value(
    count_dict_feature1, item_feature, odfv_config, udf_config, node_code_gen_output_factory
):
    """Test derive_on_demand_view_code"""
    # test on two operands
    node = GetValueFromDictionaryNode(**NODE_PARAMS, parameters={})
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": count_dict_feature1, "feat2": item_feature},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series([np.nan, np.nan, np.nan, 1.0, np.nan, 0.5, np.nan]),
    )

    # test on one operand & one scalar value
    node = GetValueFromDictionaryNode(**NODE_PARAMS, parameters={"value": "b"})

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs[:1],  # single input only
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs[:1],  # single input only
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": count_dict_feature1, "feat2": item_feature},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series([np.nan, np.nan, np.nan, 1.0, 2.0, np.nan, np.nan]),
    )


@pytest.mark.parametrize(
    "node_class, node_params, series_param_expected_values, scalar_param_expected_values",
    [
        (
            GetRankFromDictionaryNode,
            {"descending": False},
            pd.Series([np.nan, np.nan, np.nan, 1, 1, 1, 2, 2, np.nan]),
            pd.Series([np.nan, np.nan, 1, 1, 1, 2, 2, np.nan, np.nan]),
        ),
        (
            GetRankFromDictionaryNode,
            {"descending": True},
            pd.Series([np.nan, np.nan, np.nan, 1, 1, 2, 1, 2, np.nan]),
            pd.Series([np.nan, np.nan, 1, 1, 1, 1, 1, np.nan, np.nan]),
        ),
        (
            GetRelativeFrequencyFromDictionaryNode,
            {},
            pd.Series([
                np.nan,
                np.nan,
                np.nan,
                1.0 / 3,
                1.0 / 3,
                1.0 / 3,
                2.0 / 3,
                1.0 / 3.5,
                np.nan,
            ]),
            pd.Series([
                np.nan,
                np.nan,
                1.0 / 3,
                1.0 / 3,
                1.0 / 3,
                2.0 / 3,
                2.0 / 3,
                np.nan,
                np.nan,
            ]),
        ),
    ],
)
def test_derive_on_demand_view_code__dictionary_get_rank(
    rank_feat,
    rank_key_feat,
    node_class,
    node_params,
    odfv_config,
    udf_config,
    series_param_expected_values,
    scalar_param_expected_values,
    node_code_gen_output_factory,
):
    """Test derive_on_demand_view_code"""
    # test on two operands
    node = node_class(**NODE_PARAMS, parameters=node_params)
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": rank_feat, "feat2": rank_key_feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=series_param_expected_values,
    )

    # test on one operand & one scalar value
    node = node_class(**NODE_PARAMS, parameters={**node_params, "value": "b"})

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs[:1],  # single input only
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs[:1],  # single input only
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": rank_feat, "feat2": rank_key_feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=scalar_param_expected_values,
    )
