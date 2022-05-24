import pytest

from featurebyte.execution_graph.graph import ExecutionGraph


class ResettableExecutionGraph(ExecutionGraph):
    @classmethod
    def clear_instances(cls):
        del cls.__class__._instances


@pytest.fixture()
def execution_graph():
    yield ResettableExecutionGraph()
    ResettableExecutionGraph.clear_instances()


def test_add_operation__duplicated_node(execution_graph):
    node_input = execution_graph.add_operation(node_type="input", node_params={}, input_nodes=[])
    assert execution_graph.to_dict() == {
        "nodes": {"input_1": {"type": "input", "parameters": {}}},
        "edges": {},
    }

    node_proj_a_first = execution_graph.add_operation(
        node_type="project", node_params={"columns": ["a"]}, input_nodes=[node_input]
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}},
            "project_1": {"type": "project", "parameters": {"columns": ["a"]}},
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert execution_graph.to_dict() == expected_graph

    node_proj_a_second = execution_graph.add_operation(
        node_type="project", node_params={"columns": ["a"]}, input_nodes=[node_input]
    )
    assert execution_graph.to_dict() == expected_graph
    assert node_proj_a_first == node_proj_a_second
