"""
Test for StaticSourceTableService
"""
import pytest

from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.query_graph.model.common_table import TabularSource


@pytest.fixture(name="static_source_table_from_source_table")
def static_source_table_from_source_table_fixture(event_table, user):
    """
    Fixture for an StaticSourceTable from a source table
    """
    request_input = SourceTableRequestInput(source=event_table.tabular_source)
    location = TabularSource(
        **{
            "feature_store_id": event_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "fb_database",
                "schema_name": "fb_schema",
                "table_name": "fb_materialized_table",
            },
        }
    )
    return StaticSourceTableModel(
        name="static_source_table_from_source_table",
        location=location,
        request_input=request_input,
        columns_info=[
            {"name": "a", "dtype": "INT"},
            {"name": "b", "dtype": "INT"},
            {"name": "c", "dtype": "INT"},
        ],
        num_rows=1000,
        most_recent_point_in_time="2023-01-15T10:00:00",
        user_id=user.id,
    )


@pytest.mark.asyncio
async def test_create_static_source_table_from_source_table(
    static_source_table_from_source_table, static_source_table_service
):
    """
    Test creating an StaticSourceTable from a source table
    """
    await static_source_table_service.create_document(static_source_table_from_source_table)
    loaded_table = await static_source_table_service.get_document(
        static_source_table_from_source_table.id
    )
    loaded_table_dict = loaded_table.dict(exclude={"created_at", "updated_at"})
    expected_dict = static_source_table_from_source_table.dict(exclude={"created_at", "updated_at"})
    assert expected_dict == loaded_table_dict
