"""
Test models#target module.
"""
import pytest

from featurebyte.models.target import TargetModel
from featurebyte.query_graph.graph import QueryGraph


@pytest.mark.skip(reason="Target namespace is not implemented yet.")
def test_duration_validator(snowflake_event_table):
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
        tabular_source=snowflake_event_table.tabular_source,
    )

    # Fails when duration is invalid.
    with pytest.raises(ValueError) as exc:
        TargetModel(
            graph=graph,
            node_name="node name",
            horizon="random",
            entity_ids=[],
            tabular_source=snowflake_event_table.tabular_source,
        )
    assert "horizon" in str(exc)
