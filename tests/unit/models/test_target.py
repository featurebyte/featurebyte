"""
Test models#target module.
"""
import pytest

from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph


def test_duration_validator():
    """
    Test duration validator
    """
    graph = QueryGraph()

    # No failure
    TargetModel(
        graph=graph,
        node_name="node name",
        window="7d",
        blind_spot="1d",
        entity_ids=[],
    )

    # Fails when blind_spot is invalid.
    with pytest.raises(ValueError) as exc:
        TargetModel(
            graph=graph,
            node_name="node name",
            window="7d",
            blind_spot="random",
            entity_ids=[],
        )
    assert "blind_spot" in str(exc)

    # Fails when duration is invalid.
    with pytest.raises(ValueError) as exc:
        TargetModel(
            graph=graph,
            node_name="node name",
            window="random",
            blind_spot="1d",
            entity_ids=[],
        )
    assert "window" in str(exc)
