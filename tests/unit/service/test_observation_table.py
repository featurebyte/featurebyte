"""
Test for ObservationTableService
"""
import textwrap
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pandas as pd
import pytest

from featurebyte.enum import SourceType
from featurebyte.exception import (
    MissingPointInTimeColumnError,
    UnsupportedPointInTimeColumnTypeError,
)
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.session.base import BaseSession


@pytest.fixture(name="observation_table_from_source_table")
def observation_table_from_source_table_fixture(event_table, user):
    """
    Fixture for an ObservationTable from a source table
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
    return ObservationTableModel(
        name="observation_table_from_source_table",
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
        if "stats" in query:
            return pd.DataFrame(
                {
                    "dtype": ["timestamp", "int"],
                    "unique": [5, 2],
                    "min": ["2023-01-01T10:00:00+08:00", 1],
                    "max": ["2023-01-15T10:00:00+08:00", 10],
                },
            )
        if "interval" in query:
            # TODO:
            return pd.DataFrame(
                {}
            )
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
async def test_create_observation_table_from_source_table(
    observation_table_from_source_table, observation_table_service, catalog
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
    expected_dict["catalog_id"] = catalog.id
    assert expected_dict == loaded_table_dict


@pytest.mark.asyncio
async def test_validate__missing_point_in_time(
    observation_table_service, snowflake_feature_store, table_details
):
    """
    Test validation of missing point in time
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"a": "INT", "b": "FLOAT", "not_point_in_time": "VARCHAR"}

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        source_type=SourceType.SNOWFLAKE,
    )
    with pytest.raises(MissingPointInTimeColumnError):
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details, snowflake_feature_store
        )


@pytest.mark.asyncio
async def test_validate__most_recent_point_in_time(
    observation_table_service,
    db_session,
    table_details,
    cust_id_entity,
    snowflake_feature_store,
    insert_credential,
):
    """
    Test validate_materialized_table_and_get_metadata triggers expected query
    """
    _ = cust_id_entity, insert_credential
    with mock.patch(
        "featurebyte.service.preview.PreviewService._get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = (snowflake_feature_store, db_session)
        metadata = await observation_table_service.validate_materialized_table_and_get_metadata(
            db_session, table_details, snowflake_feature_store
        )

        expected_query = textwrap.dedent(
            """
            WITH data AS (
              SELECT
                "POINT_IN_TIME" AS "POINT_IN_TIME",
                "cust_id" AS "cust_id"
              FROM "fb_database"."fb_schema"."fb_table"
            ), casted_data AS (
              SELECT
                CAST("POINT_IN_TIME" AS STRING) AS "POINT_IN_TIME",
                CAST("cust_id" AS STRING) AS "cust_id"
              FROM data
            ), stats AS (
              SELECT
                COUNT(DISTINCT "POINT_IN_TIME") AS "unique__0",
                MIN("POINT_IN_TIME") AS "min__0",
                MAX("POINT_IN_TIME") AS "max__0",
                COUNT(DISTINCT "cust_id") AS "unique__1",
                NULL AS "min__1",
                NULL AS "max__1"
              FROM data
            )
            SELECT
              'TIMESTAMP' AS "dtype__0",
              stats."unique__0",
              stats."min__0",
              stats."max__0",
              'VARCHAR' AS "dtype__1",
              stats."unique__1",
              stats."min__1",
              stats."max__1"
            FROM stats
            """
        ).strip()
        query = db_session.execute_query.call_args[0][0]
        assert query == expected_query

        assert metadata == {
            "columns_info": [
                {"name": "POINT_IN_TIME", "dtype": "TIMESTAMP", "entity_id": None},
                {
                    "name": "cust_id",
                    "dtype": "VARCHAR",
                    "entity_id": cust_id_entity.id,
                },
            ],
            "least_recent_point_in_time": "2023-01-01T02:00:00",
            "most_recent_point_in_time": "2023-01-15T02:00:00",
            "num_rows": 1000,
            "entity_column_name_to_count": {"cust_id": 2},
            # TODO: fix this
            "entity_column_name_to_min_interval_secs": {"cust_id": 1},
        }


@pytest.mark.asyncio
async def test_validate__supported_type_point_in_time(
    observation_table_service, table_details, snowflake_feature_store
):
    """
    Test validate_materialized_table_and_get_metadata validates the type of point in time column
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"POINT_IN_TIME": "VARCHAR", "cust_id": "VARCHAR"}

    mock_db_session = Mock(
        name="mock_session",
        spec=BaseSession,
        list_table_schema=Mock(side_effect=mock_list_table_schema),
        source_type=SourceType.SNOWFLAKE,
    )
    with pytest.raises(UnsupportedPointInTimeColumnTypeError) as exc:
        await observation_table_service.validate_materialized_table_and_get_metadata(
            mock_db_session, table_details, snowflake_feature_store
        )

    assert str(exc.value) == "Point in time column should have timestamp type; got VARCHAR"


@pytest.mark.asyncio
async def test_request_input_get_row_count(observation_table_from_source_table, db_session):
    """
    Test get_row_count triggers expected query
    """
    db_session.execute_query = AsyncMock(return_value=pd.DataFrame({"row_count": [1000]}))

    row_count = await observation_table_from_source_table.request_input.get_row_count(
        db_session,
        observation_table_from_source_table.request_input.get_query_expr(db_session.source_type),
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


def test_get_minimum_iet_sql_expr(observation_table_service, table_details):
    """
    Test get_minimum_iet_sql_expr
    """
    expr = observation_table_service.get_minimum_iet_sql_expr(["entity"], table_details, SourceType.SNOWFLAKE)
    expr_sql = expr.sql(pretty=True)
    expected_query = textwrap.dedent(
        """
        SELECT
          MIN("interval")
        FROM (
          SELECT
            DATEDIFF(microsecond, "PREVIOUS_POINT_IN_TIME", "POINT_IN_TIME") AS "interval"
          FROM (
            SELECT
              LAG('POINT_IN_TIME') OVER (PARTITION BY 'entity' ORDER BY 'POINT_IN_TIME' NULLS LAST) AS "PREVIOUS_POINT_IN_TIME",
              POINT_IN_TIME
            FROM "fb_database"."fb_schema"."fb_table"
          )
        )
        """
    ).strip()
    assert expr_sql == expected_query
