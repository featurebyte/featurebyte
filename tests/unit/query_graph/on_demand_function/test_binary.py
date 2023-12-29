"""
Test the binary nodes in the on-demand view code generation.
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte.enum import DBVarType
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
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr

NODE_PARAMS = {"name": "node_name", "parameters": {"value": None}}


def evaluate_and_compare_result(feat1, feat2, odfv_expr, odff_expr):
    """Evaluate the odfv & odff expressions & compare the results"""
    # check the odfv expression can be evaluated
    out_odfv = eval(odfv_expr)

    # check the odff expression can be evaluated
    out_vals = []
    for feat1, feat2 in zip(feat1, feat2):
        _ = feat1, feat2
        out_vals.append(eval(odff_expr))
    out_odff = pd.Series(out_vals)

    # check the consistency between two expressions
    pd.testing.assert_series_equal(out_odfv, out_odff)


@pytest.mark.parametrize(
    "node, expected_odfv_expr, expected_odff_expr",
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
def test_derive_on_demand_function(node, expected_odfv_expr, expected_odff_expr):
    """Test derive_on_demand_view_code"""
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]

    odfv_config = OnDemandViewCodeGenConfig()
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    odff_config = OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT)
    odff_stats, odff_out_var = node.derive_on_demand_function_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=odff_config,
    )

    assert odfv_out_var == "feat"
    assert odff_out_var == "feat"
    expected_odff_expr = f"np.nan if pd.isna(feat1) or pd.isna(feat2) else {expected_odff_expr}"
    expected_odfv_expr = f"pd.Series(np.where(pd.isna(feat1) | pd.isna(feat2), np.nan, {expected_odfv_expr}), index=feat1.index)"
    if isinstance(node, BinaryOpWithBoolOutputNode):
        expected_odfv_expr = (
            f"{expected_odfv_expr}.apply(lambda x: np.nan if pd.isna(x) else bool(x))"
        )

    assert odff_stats == [("feat", expected_odff_expr)]
    assert odfv_stats == [("feat", expected_odfv_expr)]

    # check odfv statements & expression
    if isinstance(node, BinaryOpWithBoolOutputNode):
        feat1 = pd.Series([True, False, False, np.nan])
        feat2 = pd.Series([True, True, np.nan, False])
    else:
        feat1 = pd.Series([1, 2, 3, np.nan, 5])
        feat2 = pd.Series([1, 2, 3, 4, np.nan])

    # evaluate expression
    evaluate_and_compare_result(
        feat1=feat1, feat2=feat2, odfv_expr=expected_odfv_expr, odff_expr=expected_odff_expr
    )


def test_derive_on_demand_function__isin_node():
    """Test derive_on_demand_view_code (isin node)"""
    node = IsInNode(**NODE_PARAMS)
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]

    odfv_config = OnDemandViewCodeGenConfig()
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    expected_odfv_expr = "feat1.combine(feat2, lambda x, y: False if pd.isna(x) or not isinstance(y, list) else x in y)"
    assert odfv_out_var == "feat"
    assert odfv_stats == [("feat", expected_odfv_expr)]

    odff_config = OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT)
    odff_stats, odff_out_var = node.derive_on_demand_function_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=odff_config,
    )
    expected_odff_expr = (
        "False if pd.isna(feat1) or not isinstance(feat2, list) else feat1 in feat2"
    )
    assert odff_out_var == "feat"
    assert odff_stats == [("feat", expected_odff_expr)]

    # evaluate the expression & check output
    feat1 = pd.Series([1, 2, 3, np.nan, 1])
    feat2 = pd.Series([[1], [1, 2], [1], [], np.nan])

    # evaluate expression
    evaluate_and_compare_result(
        feat1=feat1, feat2=feat2, odfv_expr=expected_odfv_expr, odff_expr=expected_odff_expr
    )

    # test when the value is a list (single not input)
    node = IsInNode(**{**NODE_PARAMS, "parameters": {"value": [1, 3]}})
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    expected_odfv_expr = (
        "pd.Series(np.where(pd.isna(feat1), np.nan, feat1.isin([1, 3])), index=feat1.index)"
        ".apply(lambda x: np.nan if pd.isna(x) else bool(x))"
    )
    assert odfv_out_var == "feat"
    assert odfv_stats == [("feat", expected_odfv_expr)]

    odff_stats, odff_out_var = node.derive_on_demand_function_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=odff_config,
    )
    expected_odff_expr = "np.nan if pd.isna(feat1) else feat1 in [1, 3]"
    assert odff_out_var == "feat"
    assert odff_stats == [("feat", expected_odff_expr)]

    evaluate_and_compare_result(feat1, feat2, expected_odfv_expr, expected_odff_expr)
