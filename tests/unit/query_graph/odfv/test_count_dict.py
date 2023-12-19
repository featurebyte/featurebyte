"""
Test the count dictionary nodes in the on-demand view code generation.
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte.query_graph.node.binary import IsInNode
from featurebyte.query_graph.node.count_dict import (
    CosineSimilarityNode,
    CountDictTransformNode,
    DictionaryKeysNode,
    GetRankFromDictionaryNode,
    GetRelativeFrequencyFromDictionaryNode,
    GetValueFromDictionaryNode,
)
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
)

NODE_PARAMS = {"name": "node_name"}


@pytest.fixture(name="count_dict_feature1")
def fixture_count_dict_feature1():
    """Fixture for the count dict feature"""
    return pd.Series(
        [
            None,
            {"a": 1},
            {"a": 1, "b": 1, "c": 1},
            {"a": 1, "b": 2, "c": 3, "__MISSING__": 4},
        ]
    )


@pytest.fixture(name="count_dict_feature2")
def fixture_count_dict_feature2():
    """Fixture for the count dict feature"""
    return pd.Series(
        [
            {"a": 1},
            None,
            {"a": 1, "b": 1, "c": 1},
            {"a": 1, "b": 2},
        ]
    )


@pytest.fixture(name="rank_feat")
def fixture_rank_feat():
    """Fixture for testing get rank"""
    return pd.Series(
        [
            None,
            {"a": 1},
            {"a": 1, "b": 1, "c": 1},
            {"a": 1, "b": 1, "c": 1},
            {"a": 1, "b": 1, "c": 1},
            {"a": 1, "b": 2},
            {"a": 1, "b": 2},
        ]
    )


@pytest.fixture(name="item_feature")
def fixture_item_feature():
    """Fixture for the item feature"""
    return pd.Series(
        [
            "a",
            None,
            "b",
            "d",
        ]
    )


@pytest.fixture(name="rank_key_feat")
def fixture_rank_key_feat():
    """Fixture for key for get rank"""
    return pd.Series(
        [
            "a",
            None,
            "non_existing_key",
            "a",
            "b",
            "a",
            "b",
        ]
    )


@pytest.mark.parametrize(
    "node_params, expected_values",
    [
        (
            {"parameters": {"transform_type": "entropy"}},
            pd.Series([np.nan, 0, 1.098612, 1.279854]),
        ),
        (
            {"parameters": {"transform_type": "most_frequent"}},
            pd.Series([np.nan, "a", "a", "__MISSING__"]),
        ),
        (
            {"parameters": {"transform_type": "key_with_highest_value"}},
            pd.Series([np.nan, "a", "a", "__MISSING__"]),
        ),
        (
            {"parameters": {"transform_type": "key_with_lowest_value"}},
            pd.Series([np.nan, "a", "a", "a"]),
        ),
        (
            {"parameters": {"transform_type": "unique_count", "include_missing": True}},
            pd.Series([np.nan, 1, 3, 4]),
        ),
        (
            {"parameters": {"transform_type": "unique_count", "include_missing": False}},
            pd.Series([np.nan, 1, 3, 3]),
        ),
    ],
)
def test_derive_on_demand_view_code__count_dict_transform(
    node_params, expected_values, count_dict_feature1
):
    """Test derive_on_demand_view_code"""
    node = CountDictTransformNode(**NODE_PARAMS, **node_params)
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat"]')],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()

    local_vars = {"df": pd.DataFrame({"feat": count_dict_feature1})}
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values.name = "feature"
    pd.testing.assert_series_equal(out_df["feature"], expected_values)


def test_derive_on_demand_view_code__cosine_similarity(count_dict_feature1, count_dict_feature2):
    """Test derive_on_demand_view_code"""
    node = CosineSimilarityNode(**NODE_PARAMS)
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[
            VariableNameStr('df["feat1"]'),
            VariableNameStr('df["feat2"]'),
        ],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()

    local_vars = {
        "df": pd.DataFrame(
            {
                "feat1": count_dict_feature1,
                "feat2": count_dict_feature2,
            }
        )
    }
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values = pd.Series([np.nan, np.nan, 1.0, 0.408248], name="feature")
    pd.testing.assert_series_equal(out_df["feature"], expected_values)


def test_derive_on_demand_view_code__dictionary_keys(count_dict_feature1, item_feature):
    """Test derive_on_demand_view_code"""
    dict_keys_node = DictionaryKeysNode(**NODE_PARAMS)
    is_in_node = IsInNode(**NODE_PARAMS, parameters={})
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    var_name_generator = VariableNameGenerator()
    statements = []
    stats, expr = dict_keys_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat1"]')],
        var_name_generator=var_name_generator,
        config=config,
    )
    statements += stats
    stats, expr = is_in_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat2"]'), expr],
        var_name_generator=var_name_generator,
        config=config,
    )
    statements += stats
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()

    local_vars = {
        "df": pd.DataFrame(
            {
                "feat1": count_dict_feature1,
                "feat2": item_feature,
            }
        )
    }
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values = pd.Series([False, False, True, False], name="feature")
    pd.testing.assert_series_equal(out_df["feature"], expected_values)


def test_derive_on_demand_view_code__dictionary_get_value(count_dict_feature1, item_feature):
    """Test derive_on_demand_view_code"""
    # test on two operands
    dict_value_node = GetValueFromDictionaryNode(**NODE_PARAMS, parameters={})
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = dict_value_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat1"]'), VariableNameStr('df["feat2"]')],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()

    local_vars = {
        "df": pd.DataFrame(
            {
                "feat1": count_dict_feature1,
                "feat2": item_feature,
            }
        )
    }
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values = pd.Series([None, None, 1.0, None], name="feature")
    pd.testing.assert_series_equal(out_df["feature"], expected_values)

    # test on one operand & one scalar value
    dict_value_node = GetValueFromDictionaryNode(**NODE_PARAMS, parameters={"value": "b"})
    statements, expr = dict_value_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat1"]')],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values = pd.Series([None, None, 1.0, 2.0], name="feature")
    pd.testing.assert_series_equal(out_df["feature"], expected_values)


@pytest.mark.parametrize(
    "node_class, node_params, series_param_expected_values, scalar_param_expected_values",
    [
        (
            GetRankFromDictionaryNode,
            {"descending": False},
            pd.Series([None, None, None, 1, 1, 1, 2], name="feature"),
            pd.Series([None, None, 1, 1, 1, 2, 2], name="feature"),
        ),
        (
            GetRankFromDictionaryNode,
            {"descending": True},
            pd.Series([None, None, None, 1, 1, 2, 1], name="feature"),
            pd.Series([None, None, 1, 1, 1, 1, 1], name="feature"),
        ),
        (
            GetRelativeFrequencyFromDictionaryNode,
            {},
            pd.Series([np.nan, np.nan, np.nan, 1.0 / 3, 1.0 / 3, 1.0 / 3, 2.0 / 3], name="feature"),
            pd.Series(
                [np.nan, np.nan, 1.0 / 3, 1.0 / 3, 1.0 / 3, 2.0 / 3, 2.0 / 3], name="feature"
            ),
        ),
    ],
)
def test_derive_on_demand_view_code__dictionary_get_rank(
    rank_feat,
    rank_key_feat,
    node_class,
    node_params,
    series_param_expected_values,
    scalar_param_expected_values,
):
    """Test derive_on_demand_view_code"""
    # test on two operands
    dict_value_node = node_class(**NODE_PARAMS, parameters=node_params)
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = dict_value_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat1"]'), VariableNameStr('df["feat2"]')],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()

    local_vars = {
        "df": pd.DataFrame(
            {
                "feat1": rank_feat,
                "feat2": rank_key_feat,
            }
        )
    }
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    pd.testing.assert_series_equal(out_df["feature"], series_param_expected_values)

    # test on one operand & one scalar value
    dict_value_node = node_class(**NODE_PARAMS, parameters={**node_params, "value": "b"})
    statements, expr = dict_value_node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr('df["feat1"]')],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(
        statements=statements + [(VariableNameStr('output_df["feature"]'), expr)],
        template="on_demand_view.tpl",
    )
    codes = code_gen.generate(
        input_df_name=config.input_df_name,
        output_df_name=config.output_df_name,
        function_name=config.on_demand_function_name,
    ).strip()
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    pd.testing.assert_series_equal(out_df["feature"], scalar_param_expected_values)
