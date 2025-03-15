"""Test observation table model."""

from bson import ObjectId

from featurebyte.models.observation_table import ObservationTableModel, SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


def test_observation_table_model():
    """Test observation table model."""
    feature_store_id = ObjectId()
    table_details = TableDetails(
        database_name="db",
        schema_name="schema",
        table_name="table",
    )
    tabular_source = TabularSource(
        feature_store_id=feature_store_id,
        table_details=table_details,
    )
    observation_table = ObservationTableModel(
        name="observation_table",
        location=tabular_source,
        columns_info=[],
        num_rows=1234,
        request_input=SourceTableObservationInput(
            source=tabular_source,
        ),
        most_recent_point_in_time="2021-01-01",
    )
    assert observation_table.warehouse_tables == [table_details]

    another_table_details = table_details.model_copy()
    another_table_details.table_name = "another_table"
    another_observation_table = ObservationTableModel(**{
        **observation_table.model_dump(),
        "table_with_missing_data": another_table_details,
    })
    assert another_observation_table.warehouse_tables == [table_details, another_table_details]
