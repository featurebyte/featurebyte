"""
Test observation table utils
"""

import textwrap

from featurebyte import Context, get_version
from featurebyte.api.obs_table.utils import get_definition_for_obs_table_creation_from_view


def test_get_definition_for_obs_table_creation_from_view(
    catalog,
    snowflake_event_table,
    cust_id_entity,
    patched_observation_table_service,
    snowflake_execute_query_for_materialized_table,
):
    """
    Test get_definition_for_obs_table_creation_from_view
    """
    _ = catalog, patched_observation_table_service, snowflake_execute_query_for_materialized_table
    entity_names = [cust_id_entity.name]
    context = Context.create(name="test_context", primary_entity=entity_names)

    view = snowflake_event_table.get_view()
    pruned_graph, mapped_node = view.extract_pruned_graph_and_node()
    definition = get_definition_for_obs_table_creation_from_view(
        graph=pruned_graph,
        node=mapped_node,
        name="test_obs_table",
        sample_rows=100,
        columns=["cust_id", "col_float"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        context_name=context.name,
        skip_entity_validation_checks=True,
        primary_entities=[cust_id_entity.name],
        sample_from_timestamp="2021-01-01T13:15:00",
    )
    version = get_version()
    expected_definition = f"""
    # Generated by SDK version: {version}
    from bson import ObjectId
    from featurebyte import EventTable

    event_table = EventTable.get_by_id(ObjectId("6337f9651050ee7d5980660d"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    output = event_view.create_observation_table(
        name="test_obs_table",
        sample_rows=100,
        columns=["cust_id", "col_float"],
        columns_rename_mapping={{"event_timestamp": "POINT_IN_TIME"}},
        context_name="test_context",
        skip_entity_validation_checks=True,
        primary_entities=["customer"],
        sample_from_timestamp="2021-01-01T13:15:00",
        sample_to_timestamp=None,
    )
    """
    assert textwrap.dedent(definition).strip() == textwrap.dedent(expected_definition).strip()

    # Try to run the code to test that it works
    local_vars = {}
    exec(definition, {}, local_vars)
    observation_table = local_vars["output"]
    assert observation_table.name == "test_obs_table"
