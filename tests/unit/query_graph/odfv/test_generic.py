"""
Test node's demand feature view related methods
"""
import pytest

from featurebyte.query_graph.node.generic import AliasNode, ConditionalNode
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr


@pytest.fixture(name="config")
def fixture_config():
    """Fixture for the config"""
    return OnDemandViewCodeGenConfig(
        input_df_name="df", output_df_name="df", on_demand_function_name="on_demand"
    )


def test_alias_node(config):
    """Test AliasNode derive_on_demand_view_code"""
    node = AliasNode(name="node_name", parameters={"name": "feat"})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    assert expr == "feat"


def test_conditional_node(config):
    """Test ConditionalNode derive_on_demand_view_code"""
    node = ConditionalNode(name="node_name", parameters={"value": 1})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("mask"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == [("feat1[mask]", "feat2[mask]")]
    assert expr == "feat1"

    # check on 2 inputs
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("mask")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == [("feat1[mask]", "1")]
    assert expr == "feat1"
