"""
Test on-demand view code generation of trigonometry related nodes.
"""

import numpy as np
import pandas as pd

from featurebyte.query_graph.node.metadata.sdk_code import (
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.query_graph.node.trigonometry import Atan2Node
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results


def test_derive_on_demand_view_code__atan2(odfv_config, udf_config, node_code_gen_output_factory):
    """Test derive_on_demand_view_code for atan2"""
    y = pd.Series([1.0, -1.0, 0.0, 1.0, None, 1.0])
    x = pd.Series([1.0, 1.0, 1.0, -1.0, 1.0, None])
    expected_values = pd.Series([
        np.arctan2(1.0, 1.0),
        np.arctan2(-1.0, 1.0),
        np.arctan2(0.0, 1.0),
        np.arctan2(1.0, -1.0),
        None,
        None,
    ])

    node = Atan2Node(name="node_name", parameters={})
    node_inputs = [
        VariableNameStr("y"),
        VariableNameStr("x"),
    ]
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
        input_map={
            "y": y,
            "x": x,
        },
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=expected_values,
    )
