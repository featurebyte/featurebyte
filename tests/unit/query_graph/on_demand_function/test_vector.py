"""
Test on-demand view code generation of vector related nodes.
"""
import numpy as np
import pandas as pd

from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.query_graph.node.vector import VectorCosineSimilarityNode


def test_derive_on_demand_view_code__haversine_distance():
    """Test derive_on_demand_view_code"""
    vec1 = pd.Series([[1, 0], [0, 1], np.array([]), [], np.nan])
    vec2 = pd.Series([[1, 0], [1, 0], [], np.array([]), [0, 1]])
    expected_values = pd.Series([1.0, 0.0, 0.0, 0.0, 0.0])

    node = VectorCosineSimilarityNode(name="node_name", parameters={})
    config = OnDemandViewCodeGenConfig(
        input_df_name="input_df",
        output_df_name="output_df",
        on_demand_function_name="on_demand_func",
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[
            VariableNameStr('df["vec1"]'),
            VariableNameStr('df["vec2"]'),
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

    local_vars = {"df": pd.DataFrame({"vec1": vec1, "vec2": vec2})}
    exec_codes = codes + "\n\nout_df = on_demand_func(df)"
    exec(exec_codes, local_vars)
    out_df = local_vars["out_df"]
    expected_values.name = "feature"
    pd.testing.assert_series_equal(out_df["feature"], expected_values)
