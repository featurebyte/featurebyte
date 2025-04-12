"""
Test the unary nodes in the on-demand view code generation.
"""

import numpy as np
import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.node.metadata.sdk_code import (
    NodeCodeGenOutput,
    VariableNameGenerator,
    VariableNameStr,
)
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
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results

NODE_PARAMS = {"name": "node_name"}


@pytest.mark.parametrize(
    "node, expected_odfv_expr, expected_udf_expr",
    [
        (NotNode(**NODE_PARAMS), "feat.map(lambda x: not x if pd.notnull(x) else x)", "not feat"),
        (AbsoluteNode(**NODE_PARAMS), "feat.abs()", "np.abs(feat)"),
        (SquareRootNode(**NODE_PARAMS), "np.sqrt(feat)", "np.sqrt(feat)"),
        (FloorNode(**NODE_PARAMS), "np.floor(feat)", "np.floor(feat)"),
        (CeilNode(**NODE_PARAMS), "np.ceil(feat)", "np.ceil(feat)"),
        (CosNode(**NODE_PARAMS), "np.cos(feat)", "np.cos(feat)"),
        (SinNode(**NODE_PARAMS), "np.sin(feat)", "np.sin(feat)"),
        (TanNode(**NODE_PARAMS), "np.tan(feat)", "np.tan(feat)"),
        (AcosNode(**NODE_PARAMS), "np.arccos(feat)", "np.arccos(feat)"),
        (AsinNode(**NODE_PARAMS), "np.arcsin(feat)", "np.arcsin(feat)"),
        (AtanNode(**NODE_PARAMS), "np.arctan(feat)", "np.arctan(feat)"),
        (LogNode(**NODE_PARAMS), "np.log(feat)", "np.log(feat)"),
        (ExponentialNode(**NODE_PARAMS), "np.exp(feat)", "np.exp(feat)"),
        (IsNullNode(**NODE_PARAMS), "feat.isnull()", "pd.isna(feat)"),
        (
            CastNode(**NODE_PARAMS, parameters={"type": "str", "from_dtype": DBVarType.INT}),
            "feat.map(lambda x: str(x) if pd.notnull(x) else x).astype(object)",
            "str(feat)",
        ),
        (
            CastNode(**NODE_PARAMS, parameters={"type": "float", "from_dtype": DBVarType.INT}),
            "feat.map(lambda x: float(x) if pd.notnull(x) else x)",
            "float(feat)",
        ),
        (
            CastNode(**NODE_PARAMS, parameters={"type": "int", "from_dtype": DBVarType.FLOAT}),
            "feat.map(lambda x: int(x) if pd.notnull(x) else x)",
            "int(feat)",
        ),
    ],
)
def test_derive_on_demand_view_code(
    node, odfv_config, udf_config, expected_odfv_expr, expected_udf_expr
):
    """Test derive_on_demand_view_code"""
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [NodeCodeGenOutput(var_name_or_expr=node_input) for node_input in node_inputs]

    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_out_var = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    if not isinstance(node, IsNullNode):
        expected_udf_expr = f"np.nan if pd.isna(feat) else {expected_udf_expr}"

    assert udf_stats == []
    assert udf_out_var == expected_udf_expr

    assert odfv_stats == []
    assert odfv_out_var == expected_odfv_expr

    # check the expression can be evaluated & matches expected
    feat = pd.Series([0, 1, 2, 3.1, np.nan])
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_out_var,
        udf_expr=udf_out_var,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )
