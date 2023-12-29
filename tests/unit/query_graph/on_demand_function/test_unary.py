"""
Test the unary nodes in the on-demand view code generation.
"""
import numpy as np  # this is required for eval
import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr
from featurebyte.query_graph.node.unary import (
    AbsoluteNode,
    AcosNode,
    AsinNode,
    AtanNode,
    CastNode,
    CeilNode,
    CosNode,
    ExponentialNode,
    FloorNode,
    IsNullNode,
    LogNode,
    NotNode,
    SinNode,
    SquareRootNode,
    TanNode,
)

NODE_PARAMS = {"name": "node_name"}


@pytest.mark.parametrize(
    "node, expected_expr",
    [
        (NotNode(**NODE_PARAMS), "~feat"),
        (AbsoluteNode(**NODE_PARAMS), "feat.abs()"),
        (SquareRootNode(**NODE_PARAMS), "np.sqrt(feat)"),
        (FloorNode(**NODE_PARAMS), "np.floor(feat)"),
        (CeilNode(**NODE_PARAMS), "np.ceil(feat)"),
        (CosNode(**NODE_PARAMS), "np.cos(feat)"),
        (SinNode(**NODE_PARAMS), "np.sin(feat)"),
        (TanNode(**NODE_PARAMS), "np.tan(feat)"),
        (AcosNode(**NODE_PARAMS), "np.arccos(feat)"),
        (AsinNode(**NODE_PARAMS), "np.arcsin(feat)"),
        (AtanNode(**NODE_PARAMS), "np.arctan(feat)"),
        (LogNode(**NODE_PARAMS), "np.log(feat)"),
        (ExponentialNode(**NODE_PARAMS), "np.exp(feat)"),
        (IsNullNode(**NODE_PARAMS), "feat.isnull()"),
        (
            CastNode(**NODE_PARAMS, parameters={"type": "str", "from_dtype": DBVarType.INT}),
            "feat.astype(str)",
        ),
    ],
)
def test_derive_on_demand_view_code(node, expected_expr):
    """Test derive_on_demand_view_code"""
    config = OnDemandViewCodeGenConfig(
        input_df_name="df", output_df_name="df", on_demand_function_name="on_demand"
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat = pd.Series([1, 2, 3])
    _ = feat

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == expected_expr
