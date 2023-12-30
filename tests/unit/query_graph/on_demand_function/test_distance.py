"""
Test on-demand view code generation of distance related nodes.
"""
import pandas as pd
import pytest

from featurebyte.query_graph.node.distance import HaversineNode
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
)


def test_derive_on_demand_view_code__haversine_distance():
    """Test derive_on_demand_view_code"""
    lat1 = pd.Series([40.7128, 51.5074, None, 40.7128, 40.7128, 40.7128])
    lon1 = pd.Series([-74.0060, -0.1278, 74.0060, None, -74.0060, -74.0060])
    lat2 = pd.Series([34.0522, 48.8566, 34.0522, 34.0522, None, 34.0522])
    lon2 = pd.Series([-118.2437, 2.3522, -118.2437, -118.2437, -118.2437, None])
    expected_values = pd.Series([3935.746255, 343.556060, None, None, None, None])

    node = HaversineNode(name="node_name", parameters={})
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[
            VariableNameStr('df["lat1"]'),
            VariableNameStr('df["lon1"]'),
            VariableNameStr('df["lat2"]'),
            VariableNameStr('df["lon2"]'),
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

    local_vars = {"df": pd.DataFrame({"lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2})}
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values.name = "feature"
    pd.testing.assert_series_equal(out_df["feature"], expected_values)
