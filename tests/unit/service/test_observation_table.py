"""
Test for ObservationTableService
"""
import pytest

from featurebyte.models.observation_table import ObservationTableModel, SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource


@pytest.fixture(name="observation_table_from_source_table")
def observation_table_from_source_table_fixture(event_table):
    observation_input = SourceTableObservationInput(source=event_table.tabular_source)
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
    return ObservationTableModel(
        name="observation_table_from_source_table",
        location=location,
        observation_input=observation_input,
        column_names=["POINT_IN_TIME", "user_id"],
    )


@pytest.mark.asyncio
async def test_create_observation_table_from_source_table(
    observation_table_from_source_table, observation_table_service
):
    """
    Test creating an ObservationTable from a source table
    """
    await observation_table_service.create_document(observation_table_from_source_table)
    loaded_table = await observation_table_service.get_document(
        observation_table_from_source_table.id
    )
    loaded_table_dict = loaded_table.dict(exclude={"created_at", "updated_at"})
    expected_dict = observation_table_from_source_table.dict(exclude={"created_at", "updated_at"})
    assert expected_dict == loaded_table_dict
