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
        window="7d",
        entity_ids=[],
        tabular_source=snowflake_event_table.tabular_source,
    )

    # Fails when duration is invalid.
    with pytest.raises(ValueError) as exc:
        TargetModel(
            graph=graph,
            node_name="node name",
            window="random",
            entity_ids=[],
            tabular_source=snowflake_event_table.tabular_source,
        )
    assert "window" in str(exc)


@pytest.fixture(name="lookup_target")
def lookup_target_fixture(snowflake_event_view_with_entity):
    """
    Lookup target fixture
    """
    return snowflake_event_view_with_entity["col_float"].as_target(
        "lookup_target", "7d", fill_value=None
    )


@pytest.mark.asyncio
async def test_derive_window(float_target, lookup_target, app_container):
    """
    Test that derive window works as expected
    """
    float_target.save()
    target = await app_container.target_service.get_document(document_id=float_target.id)
    assert target.derive_window() == "1d"

    lookup_target.save()
    lookup_target_doc = await app_container.target_service.get_document(
        document_id=lookup_target.id
    )
    assert lookup_target_doc.derive_window() == "7d"
