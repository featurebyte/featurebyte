"""
Test on-demand view code generation of vector related nodes.
"""

import numpy as np
import pandas as pd

from featurebyte.query_graph.node.metadata.sdk_code import (
    NodeCodeGenOutput,
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.query_graph.node.vector import VectorCosineSimilarityNode
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results


def test_derive_on_demand_view_code__haversine_distance(odfv_config):
    """Test derive_on_demand_view_code"""
    vec1 = pd.Series([[1, 0], [0, 1], np.array([]), [], np.nan])
    vec2 = pd.Series([[1, 0], [1, 0], [], np.array([]), [0, 1]])
    expected_values = pd.Series([1.0, 0.0, 0.0, 0.0, 0.0])

    node = VectorCosineSimilarityNode(name="node_name", parameters={})
    node_inputs = [VariableNameStr("vec1"), VariableNameStr("vec2")]
    node_inputs = [NodeCodeGenOutput(var_name_or_expr=node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    evaluate_and_compare_odfv_and_udf_results(
        input_map={
            "vec1": vec1,
            "vec2": vec2,
        },
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=expected_values,
    )
