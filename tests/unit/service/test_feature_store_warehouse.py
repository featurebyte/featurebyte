"""
Test cases for feature_store_warehouse.py
"""
import pytest

from featurebyte.query_graph.model.table import TableSpec


@pytest.mark.parametrize("featurebyte_schema", [True, False])
@pytest.mark.asyncio
async def test_list_tables(
    feature_store_warehouse_service,
    feature_store,
    mock_get_feature_store_session,
    featurebyte_schema,
):
    """
    Test list_tables
    """
    mock_session = mock_get_feature_store_session.return_value
    mock_session.source_type = "snowflake"
    mock_session.no_schema_error = Exception
    table_specs = [
        TableSpec(name="__table1", description="table1"),
        TableSpec(name="FEATURE_TABLE_CACHE", description="feature table cache"),
        TableSpec(name="OBSERVATION_TABLE", description="observation table"),
        TableSpec(name="HISTORICAL_FEATURE_TABLE", description="historical feature table"),
        TableSpec(name="BATCH_REQUEST_TABLE", description="batch request table"),
        TableSpec(name="BATCH_FEATURE_TABLE", description="batch feature table"),
        TableSpec(name="TARGET_TABLE", description="target table"),
        TableSpec(name="table1", description="table1"),
        TableSpec(name="table2", description="table2"),
    ]

    if not featurebyte_schema:
        mock_session.execute_query.side_effect = Exception

    mock_session.list_tables.return_value = table_specs
    tables = await feature_store_warehouse_service.list_tables(
        feature_store=feature_store,
        database_name="db_name",
        schema_name="schema_name",
    )
    assert (
        mock_session.execute_query.call_args[0][0]
        == 'SELECT\n  "FEATURE_STORE_ID"\nFROM "db_name"."schema_name"."METADATA_SCHEMA"'
    )
    if featurebyte_schema:
        assert tables == table_specs[2:7]
    else:
        assert tables == table_specs[1:]
