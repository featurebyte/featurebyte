"""
Test the binary nodes in the on-demand view code generation.
"""

import numpy as np
import pandas as pd
import pytest

from featurebyte.query_graph.node.base import BinaryOpWithBoolOutputNode
from featurebyte.query_graph.node.binary import (
    AddNode,
    AndNode,
    DivideNode,
    EqualNode,
    GreaterEqualNode,
    GreaterThanNode,
    IsInNode,
    LessEqualNode,
    LessThanNode,
    ModuloNode,
    MultiplyNode,
    NotEqualNode,
    OrNode,
    PowerNode,
    SubtractNode,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    VariableNameGenerator,
    VariableNameStr,
)
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results

NODE_PARAMS = {"name": "node_name", "parameters": {"value": None}}


@pytest.mark.parametrize(
    "node, expected_odfv_expr, expected_udf_expr",
    [
        (AndNode(**NODE_PARAMS), "feat1 & feat2", "feat1 & feat2"),
        (OrNode(**NODE_PARAMS), "feat1 | feat2", "feat1 | feat2"),
        (EqualNode(**NODE_PARAMS), "feat1 == feat2", "feat1 == feat2"),
        (NotEqualNode(**NODE_PARAMS), "feat1 != feat2", "feat1 != feat2"),
        (GreaterThanNode(**NODE_PARAMS), "feat1 > feat2", "feat1 > feat2"),
        (GreaterEqualNode(**NODE_PARAMS), "feat1 >= feat2", "feat1 >= feat2"),
        (LessThanNode(**NODE_PARAMS), "feat1 < feat2", "feat1 < feat2"),
        (LessEqualNode(**NODE_PARAMS), "feat1 <= feat2", "feat1 <= feat2"),
        (AddNode(**NODE_PARAMS), "feat1 + feat2", "feat1 + feat2"),
        (SubtractNode(**NODE_PARAMS), "feat1 - feat2", "feat1 - feat2"),
        (MultiplyNode(**NODE_PARAMS), "feat1 * feat2", "feat1 * feat2"),
        (DivideNode(**NODE_PARAMS), "feat1 / feat2", "feat1 / feat2"),
        (ModuloNode(**NODE_PARAMS), "feat1 % feat2", "feat1 % feat2"),
        (PowerNode(**NODE_PARAMS), "feat1.pow(feat2)", "np.power(feat1, feat2)"),
    ],
)
def test_derive_on_demand_function(
    node,
    odfv_config,
    udf_config,
    expected_odfv_expr,
    expected_udf_expr,
    node_code_gen_output_factory,
):
    """Test derive_on_demand_view_code"""
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

    # check odfv statements & expression
    if isinstance(node, BinaryOpWithBoolOutputNode):
        feat1 = pd.Series([True, False, False, np.nan])
        feat2 = pd.Series([True, True, np.nan, False])
    else:
        feat1 = pd.Series([1, 2, 3, np.nan, 5])
        feat2 = pd.Series([1, 2, 3, 4, np.nan])

    # evaluate expression
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": feat1, "feat2": feat2},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )


def test_derive_on_demand_function__isin_node(
    odfv_config, udf_config, node_code_gen_output_factory
):
    """Test derive_on_demand_view_code (isin node)"""
    node = IsInNode(**NODE_PARAMS)
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    expected_odfv_expr = (
        "feat1.combine(feat2, "
        "lambda x, y: False if pd.isna(x) or not isinstance(y, list) else x in y)"
    )
    assert odfv_stats == []
    assert odfv_expr == expected_odfv_expr

    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [
        node_code_gen_output_factory(var_name_or_expr=node_input) for node_input in node_inputs
    ]
    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    expected_udf_expr = "False if pd.isna(feat1) or not isinstance(feat2, list) else feat1 in feat2"
    assert udf_stats == []
    assert udf_expr == expected_udf_expr

    # evaluate the expression & check output
    feat1 = pd.Series([1, 2, 3, np.nan, 1])
    feat2 = pd.Series([[1], [1, 2], [1], [], np.nan])

    # evaluate expression
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": feat1, "feat2": feat2},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )

    # test when the value is a list (single not input)
    node = IsInNode(**{**NODE_PARAMS, "parameters": {"value": [1, 3]}})
    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    expected_odfv_expr = (
        "pd.Series(np.where(pd.isna(feat1), np.nan, feat1.isin([1, 3])), index=feat1.index)"
        ".apply(lambda x: np.nan if pd.isna(x) else bool(x))"
    )
    assert odfv_stats == []
    assert odfv_expr == expected_odfv_expr

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    expected_udf_expr = "np.nan if pd.isna(feat1) else feat1 in [1, 3]"
    assert udf_stats == []
    assert udf_expr == expected_udf_expr

    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": feat1},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )
