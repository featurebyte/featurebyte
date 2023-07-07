"""
Test models#target module.
"""
import pytest

from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph


@pytest.mark.skip(reason="Target creation is not implemented yet")
def test_duration_validator():
    """
    Test duration validator
    """
    graph = QueryGraph()

    # No failure
    TargetModel(
        graph=graph,
        node_name="node name",
        horizon="7d",
        entity_ids=[],
    )

    # Fails when duration is invalid.
    with pytest.raises(ValueError) as exc:
        TargetModel(
            graph=graph,
            node_name="node name",
            horizon="random",
            entity_ids=[],
        )
    assert "horizon" in str(exc)
