"""
Test for StaticSourceTableService
"""
import textwrap
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from featurebyte.enum import SourceType
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base import BaseSession


@pytest.fixture(name="static_source_table_from_source_table")
def static_source_table_from_source_table_fixture(event_table):
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
        return {"POINT_IN_TIME": "TIMESTAMP", "cust_id": "VARCHAR"}

    async def execute_query(*args, **kwargs):
        query = args[0]
        _ = kwargs
        if "COUNT(*)" in query:
            return pd.DataFrame({"row_count": [1000]})
        raise NotImplementedError(f"Unexpected query: {query}")

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        execute_query=Mock(side_effect=execute_query),
        source_type=SourceType.SNOWFLAKE,
    )
    return mock_db_session


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


@pytest.mark.asyncio
async def test_request_input_get_row_count(static_source_table_from_source_table, db_session):
    """
    Test get_row_count triggers expected query
    """
    db_session.execute_query = AsyncMock(return_value=pd.DataFrame({"row_count": [1000]}))

    row_count = await static_source_table_from_source_table.request_input.get_row_count(
        db_session,
        static_source_table_from_source_table.request_input.get_query_expr(db_session.source_type),
    )
    assert row_count == 1000

    expected_query = textwrap.dedent(
        """
        SELECT
          COUNT(*) AS "row_count"
        FROM (
          SELECT
            *
          FROM "sf_database"."sf_schema"."sf_event_table"
        )
        """
    ).strip()
    query = db_session.execute_query.call_args[0][0]
    assert query == expected_query
