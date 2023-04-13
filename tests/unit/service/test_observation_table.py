"""
Test for ObservationTableService
"""
import textwrap
from unittest.mock import Mock

import pandas as pd
import pytest

from featurebyte.enum import SourceType
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.observation_table import ObservationTableModel, SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base import BaseSession


@pytest.fixture(name="observation_table_from_source_table")
def observation_table_from_source_table_fixture(event_table):
    """
    Fixture for an ObservationTable from a source table
    """
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
        column_names=["a", "b", "c"],
        most_recent_point_in_time="2023-01-15T10:00:00",
    )


@pytest.fixture(name="table_details")
def table_details_fixture():
    """
    Fixture for a TableDetails
    """
    return TableDetails(
        database_name="fb_database",
        schema_name="fb_schema",
        table_name="fb_table",
    )


@pytest.fixture(name="db_session")
def db_session_fixture():
    """
    Fixture for a db session
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"POINT_IN_TIME": "TIMESTAMP", "cust_id": "STRING"}

    async def execute_query(*args, **kwargs):
        _ = args
        _ = kwargs
        return pd.DataFrame({"max_time": ["2023-01-15T10:00:00+08:00"]})

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        execute_query=Mock(side_effect=execute_query),
        source_type=SourceType.SNOWFLAKE,
    )
    return mock_db_session


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


@pytest.mark.asyncio
async def test_get_additional_metadata__missing_point_in_time(
    observation_table_service, table_details
):
    """
    Test validation of missing point in time
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"a": "int", "b": "float", "not_point_in_time": "string"}

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
    )
    with pytest.raises(MissingPointInTimeColumnError):
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details
        )


@pytest.mark.asyncio
async def test_get_additional_metadata__most_recent_point_in_time(
    observation_table_service, db_session, table_details
):
    """
    Test validate_materialized_table_and_get_metadata triggers expected query
    """
    metadata = await observation_table_service.validate_materialized_table_and_get_metadata(
        db_session, table_details
    )

    expected_query = textwrap.dedent(
        """
        SELECT
          MAX("POINT_IN_TIME")
        FROM "fb_database"."fb_schema"."fb_table"
        """
    ).strip()
    query = db_session.execute_query.call_args[0][0]
    assert query == expected_query

    assert metadata == {
        "column_names": ["POINT_IN_TIME", "cust_id"],
        "most_recent_point_in_time": "2023-01-15T02:00:00",
    }


@pytest.mark.asyncio
async def test_get_additional_metadata__supported_type_point_in_time(
    observation_table_service, table_details
):
    """
    Test validate_materialized_table_and_get_metadata validates the type of point in time column
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"POINT_IN_TIME": "STRING", "cust_id": "STRING"}

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
    )
    with pytest.raises(UnsupportedPointInTimeColumnTypeError) as exc:
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details
        )

    assert str(exc.value) == "Point in time column should have timestamp type; got STRING"
