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


@pytest.mark.asyncio
async def test_get_forecasted_column__lookup_target(lookup_target, app_container):
    """
    Test that get_forecasted_column extracts the correct column from a lookup target.
    """
    lookup_target.save()
    target_doc = await app_container.target_service.get_document(document_id=lookup_target.id)
    result = target_doc.get_forecasted_column()
    assert result is not None
    assert result.column_name == "col_float"
    assert result.table_id in [tid.table_id for tid in target_doc.table_id_column_names]


@pytest.mark.asyncio
async def test_get_forecasted_column__forward_aggregate_target(float_target, app_container):
    """
    Test that get_forecasted_column returns None for a forward_aggregate target
    (not created via as_target).
    """
    float_target.save()
    target_doc = await app_container.target_service.get_document(document_id=float_target.id)
    result = target_doc.get_forecasted_column()
    assert result is None


@pytest.mark.asyncio
async def test_get_forecasted_column__no_graph(snowflake_event_view_with_entity, app_container):
    """
    Test that get_forecasted_column returns None when target has no graph.
    """
    # Create a lookup target but modify the persisted doc to have no graph
    target = snowflake_event_view_with_entity["col_float"].as_target(
        "target_no_graph", "7d", fill_value=None
    )
    target.save()
    target_doc = await app_container.target_service.get_document(document_id=target.id)
    # Override internal_graph to simulate a target with no recipe
    object.__setattr__(target_doc, "internal_graph", None)
    result = target_doc.get_forecasted_column()
    assert result is None
