"""Test observation table model."""

import pytest
from bson import ObjectId

from featurebyte.models.observation_table import (
    AutomatedGenerationInput,
    ObservationTableModel,
    SourceTableObservationInput,
)
from featurebyte.models.request_input import RequestInputType
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


def test_automated_generation_input():
    """Test AutomatedGenerationInput model creation and type discriminator."""
    input_obj = AutomatedGenerationInput()
    assert input_obj.type == RequestInputType.AUTOMATED_GENERATION
    assert input_obj.type == "automated_generation"


def test_automated_generation_input_serialization_roundtrip():
    """Test AutomatedGenerationInput can be serialized and deserialized via the discriminated union."""
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
        name="automated_obs_table",
        location=tabular_source,
        columns_info=[],
        num_rows=500,
        request_input=AutomatedGenerationInput(),
        most_recent_point_in_time="2024-06-01",
    )

    # Serialize and deserialize
    dumped = observation_table.model_dump()
    assert dumped["request_input"]["type"] == "automated_generation"

    restored = ObservationTableModel(**dumped)
    assert isinstance(restored.request_input, AutomatedGenerationInput)
    assert restored.request_input.type == RequestInputType.AUTOMATED_GENERATION


@pytest.mark.asyncio
async def test_automated_generation_input_materialize_is_noop():
    """Test that AutomatedGenerationInput.materialize() is a no-op."""
    input_obj = AutomatedGenerationInput()
    # Should not raise - it's a no-op
    await input_obj.materialize(
        session=None,
        destination=None,
        feature_store=None,
        sample_rows=None,
    )
