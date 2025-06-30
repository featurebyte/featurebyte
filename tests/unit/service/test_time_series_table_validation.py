"""
Tests for TimeSeriesTableValidationService
"""

from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import TimeInterval, TimestampSchema
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDetails
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.time_series_table import TimeSeriesTableCreate
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="document_service")
def document_service_fixture(app_container):
    """
    Fixture for DocumentService
    """
    return app_container.time_series_table_service


@pytest.fixture(name="table_validation_service")
def table_validation_service_fixture(app_container, mock_snowflake_session):
    """
    TimeSeriesTableValidationService
    """
    with patch(
        "featurebyte.service.base_table_validation.SessionManagerService.get_feature_store_session",
        return_value=mock_snowflake_session,
    ):
        yield app_container.time_series_table_validation_service


@pytest.fixture(name="column_statistics_service")
def column_statistics_service_fixture(app_container):
    """
    ColumnStatisticsService fixture
    """
    return app_container.column_statistics_service


@pytest_asyncio.fixture(name="table_model")
async def table_model_fixture(document_service, feature_store):
    """
    Time series table model fixture
    """
    payload = TimeSeriesTableCreate(
        name="my_time_series_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDetails(
                name="snapshot_date",
                dtype=DBVarType.VARCHAR,
            ),
            ColumnSpecWithDetails(
                name="cust_id",
                dtype=DBVarType.INT,
            ),
        ],
        reference_datetime_column="snapshot_date",
        reference_datetime_schema=TimestampSchema(format_string="%Y-%m-%d"),
        series_id_column="cust_id",
        time_interval=TimeInterval(unit="DAY", value=1),
    )
    return await document_service.create_document(payload)


@pytest.mark.asyncio
async def test_validate_and_update(
    table_validation_service,
    column_statistics_service,
    document_service,
    table_model,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test validate_and_update
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "snapshot_date": [100]
    })
    await table_validation_service.validate_and_update(table_model.id)

    # Check executed queries
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/time_series_table_validation_service/validate_and_update.sql",
        update_fixtures,
    )

    # Check table model validation status
    updated_table_model = await document_service.get_document(table_model.id)
    updated_table_model_dict = updated_table_model.dict()
    assert updated_table_model_dict["validation"].pop("updated_at") is not None
    assert updated_table_model_dict["validation"] == {
        "status": "PASSED",
        "validation_message": None,
        "task_id": None,
    }

    # Check column statistics
    column_statistics = await column_statistics_service.get_column_statistics_info()
    assert len(column_statistics.all_column_statistics) == 1
    assert len(column_statistics.all_column_statistics[table_model.id]) == 1
    model = column_statistics.get_column_statistics(
        table_model.id, table_model.reference_datetime_column
    )
    assert model.model_dump(include={"table_id", "column_name", "stats"}) == {
        "table_id": table_model.id,
        "column_name": "snapshot_date",
        "stats": {"distinct_count": 100},
    }
