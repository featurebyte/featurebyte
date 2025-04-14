"""
Test node's demand feature view related methods
"""

from featurebyte.query_graph.node.generic import AliasNode, ConditionalNode, FilterNode
from featurebyte.query_graph.node.metadata.sdk_code import (
    VariableNameGenerator,
    VariableNameStr,
)


def test_alias_node(odfv_config, udf_config, node_code_gen_output_factory):
    """Test AliasNode derive_on_demand_view_code"""
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]
    node = AliasNode(name="node_name", parameters={"name": "feat"})

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == "feat"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == []
    assert udf_expr == "feat"


def test_conditional_node(odfv_config, udf_config, node_code_gen_output_factory):
    """Test ConditionalNode derive_on_demand_view_code"""
    node = ConditionalNode(name="node_name", parameters={"value": 1})
    three_node_inputs = [
        VariableNameStr("feat1"),
        VariableNameStr("mask"),
        VariableNameStr("feat2"),
    ]
    three_node_inputs = [
        node_code_gen_output_factory(node_input) for node_input in three_node_inputs
    ]
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=three_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == [("feat1[mask]", "feat2[mask]")]
    assert odfv_out_var == "feat1"

    udf_stats, udf_out_var = node.derive_user_defined_function_code(
        node_inputs=three_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == [("feat", "feat2 if mask else feat1")]
    assert udf_out_var == "feat"

    # check on 2 inputs
    two_node_inputs = [VariableNameStr("feat1"), VariableNameStr("mask")]
    two_node_inputs = [node_code_gen_output_factory(node_input) for node_input in two_node_inputs]
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=two_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == [("feat1[mask]", "1")]
    assert odfv_out_var == "feat1"

    udf_stats, udf_out_var = node.derive_user_defined_function_code(
        node_inputs=two_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == [("feat", "1 if mask else feat1")]
    assert udf_out_var == "feat"


def test_filter_node(odfv_config, udf_config, node_code_gen_output_factory):
    """Test FilterNode derive_on_demand_view_code"""
    node = FilterNode(name="node_name", output_type="series")
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("mask")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_out_var == "feat1[mask].reindex(index=feat1.index)"

    udf_stats, udf_out_var = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == []
    assert udf_out_var == "feat1 if mask else np.nan"
