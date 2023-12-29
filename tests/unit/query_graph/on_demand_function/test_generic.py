"""
Test node's demand feature view related methods
"""
from featurebyte.enum import DBVarType
from featurebyte.query_graph.node.generic import AliasNode, ConditionalNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr


def test_alias_node():
    """Test AliasNode derive_on_demand_view_code"""
    node_inputs = [VariableNameStr("feat")]
    node = AliasNode(name="node_name", parameters={"name": "feat"})

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandViewCodeGenConfig(),
    )
    assert odfv_stats == []
    assert odfv_expr == "feat"

    odff_stats, odff_expr = node.derive_on_demand_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT),
    )
    assert odff_stats == []
    assert odff_expr == "feat"


def test_conditional_node():
    """Test ConditionalNode derive_on_demand_view_code"""
    node = ConditionalNode(name="node_name", parameters={"value": 1})
    three_node_inputs = [
        VariableNameStr("feat1"),
        VariableNameStr("mask"),
        VariableNameStr("feat2"),
    ]
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=three_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandViewCodeGenConfig(),
    )
    assert odfv_stats == [("feat1[mask]", "feat2[mask]")]
    assert odfv_out_var == "feat1"

    odff_stats, odff_out_var = node.derive_on_demand_function_code(
        node_inputs=three_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT),
    )
    assert odff_stats == ["feat1 = feat2 if mask else feat1"]
    assert odff_out_var == "feat1"

    # check on 2 inputs
    two_node_inputs = [VariableNameStr("feat1"), VariableNameStr("mask")]
    odfv_stats, odfv_out_var = node.derive_on_demand_view_code(
        node_inputs=two_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandViewCodeGenConfig(),
    )
    assert odfv_stats == [("feat1[mask]", "1")]
    assert odfv_out_var == "feat1"

    odff_stats, odff_out_var = node.derive_on_demand_function_code(
        node_inputs=two_node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=OnDemandFunctionCodeGenConfig(output_dtype=DBVarType.FLOAT),
    )
    assert odff_stats == ["feat1 = 1 if mask else feat1"]
    assert odff_out_var == "feat1"
