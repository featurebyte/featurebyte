"""
Test on-demand view code generation of distance related nodes.
"""

import pandas as pd

from featurebyte.query_graph.node.distance import HaversineNode
from featurebyte.query_graph.node.metadata.sdk_code import (
    NodeCodeGenOutput,
    VariableNameGenerator,
    VariableNameStr,
)
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results


def test_derive_on_demand_view_code__haversine_distance(odfv_config, udf_config):
    """Test derive_on_demand_view_code"""
    lat1 = pd.Series([40.7128, 51.5074, None, 40.7128, 40.7128, 40.7128])
    lon1 = pd.Series([-74.0060, -0.1278, 74.0060, None, -74.0060, -74.0060])
    lat2 = pd.Series([34.0522, 48.8566, 34.0522, 34.0522, None, 34.0522])
    lon2 = pd.Series([-118.2437, 2.3522, -118.2437, -118.2437, -118.2437, None])
    expected_values = pd.Series([3935.746255, 343.556060, None, None, None, None])

    node = HaversineNode(name="node_name", parameters={})
    node_inputs = [
        VariableNameStr("lat1"),
        VariableNameStr("lon1"),
        VariableNameStr("lat2"),
        VariableNameStr("lon2"),
    ]
    node_inputs = [NodeCodeGenOutput(var_name_or_expr=node_input) for node_input in node_inputs]

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
            "lat1": lat1,
            "lon1": lon1,
            "lat2": lat2,
            "lon2": lon2,
        },
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=expected_values,
    )
