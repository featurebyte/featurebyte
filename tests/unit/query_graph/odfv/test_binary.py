"""
Test the binary nodes in the on-demand view code generation.
"""
import pandas as pd
import pytest

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
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr

NODE_PARAMS = {"name": "node_name", "parameters": {"value": None}}


@pytest.mark.parametrize(
    "node, expected_expr",
    [
        (AndNode(**NODE_PARAMS), "feat1 & feat2"),
        (OrNode(**NODE_PARAMS), "feat1 | feat2"),
        (EqualNode(**NODE_PARAMS), "feat1 == feat2"),
        (NotEqualNode(**NODE_PARAMS), "feat1 != feat2"),
        (GreaterThanNode(**NODE_PARAMS), "feat1 > feat2"),
        (GreaterEqualNode(**NODE_PARAMS), "feat1 >= feat2"),
        (LessThanNode(**NODE_PARAMS), "feat1 < feat2"),
        (LessEqualNode(**NODE_PARAMS), "feat1 <= feat2"),
        (AddNode(**NODE_PARAMS), "feat1 + feat2"),
        (SubtractNode(**NODE_PARAMS), "feat1 - feat2"),
        (MultiplyNode(**NODE_PARAMS), "feat1 * feat2"),
        (DivideNode(**NODE_PARAMS), "feat1 / feat2"),
        (ModuloNode(**NODE_PARAMS), "feat1 % feat2"),
        (PowerNode(**NODE_PARAMS), "feat1.pow(feat2)"),
        (IsInNode(**NODE_PARAMS), "feat1.isin(feat2)"),
    ],
)
def test_derive_on_demand_view_code(node, expected_expr):
    """Test derive_on_demand_view_code"""
    config = OnDemandViewCodeGenConfig(
        input_df_name="df", output_df_name="df", on_demand_function_name="on_demand"
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat1 = pd.Series([1, 2, 3])
    feat2 = pd.Series([1, 2, 3])
    _ = feat1, feat2

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == expected_expr
