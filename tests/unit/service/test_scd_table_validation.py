"""
Unit tests for SCDTableValidationService
"""

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDetails
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.scd_table_validation import SCDTableValidationService
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="service")
def service_fixture(app_container) -> SCDTableValidationService:
    """
    Fixture for SCDTableValidationService
    """
    return app_container.scd_table_validation_service


@pytest.fixture(name="document_service")
def document_service_fixture(app_container) -> SCDTableService:
    """
    Fixture for SCDTableService
    """
    return app_container.scd_table_service


@pytest.fixture(name="payload_no_end_timestamp")
def payload_no_end_timestamp_fixture(feature_store):
    """
    Fixture for SCDTableCreate with no end_timestamp_column
    """
    return SCDTableCreate(
        name="my_scd_table",
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
                name="effective_date",
                dtype=DBVarType.TIMESTAMP,
            ),
            ColumnSpecWithDetails(
                name="cust_id",
                dtype=DBVarType.INT,
            ),
        ],
        natural_key_column="cust_id",
        effective_timestamp_column="effective_date",
    )


@pytest_asyncio.fixture(name="table_no_end_timestamp")
async def table_no_end_timestamp_fixture(document_service, payload_no_end_timestamp):
    """
    Fixture for SCDTableModel with no end_timestamp_column
    """
    return await document_service.create_document(payload_no_end_timestamp)


@pytest_asyncio.fixture(name="table_with_end_timestamp")
async def table_with_end_timestamp_fixture(payload_no_end_timestamp, document_service):
    """
    Fixture for SCDTableModel with end_timestamp_column
    """
    payload = payload_no_end_timestamp.copy()
    payload.columns_info.append(
        ColumnSpecWithDetails(
            name="end_date",
            dtype=DBVarType.TIMESTAMP,
        )
    )
    payload.end_timestamp_column = "end_date"
    return await document_service.create_document(payload)


@pytest.mark.asyncio
async def test_validation_query__no_end_timestamp(
    service,
    mock_snowflake_session,
    table_no_end_timestamp,
    adapter,
    update_fixtures,
):
    """
    Test active record counts query when end_timestamp_column is None
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(mock_snowflake_session, table_no_end_timestamp)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/scd_table_validation/no_end_timestamp.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validation_query__with_end_timestamp(
    service,
    mock_snowflake_session,
    table_with_end_timestamp,
    adapter,
    update_fixtures,
):
    """
    Test active record counts query when end_timestamp_column is available
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(mock_snowflake_session, table_with_end_timestamp)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/scd_table_validation/with_end_timestamp.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validation_exception__with_end_timestamp_1(
    service, table_with_end_timestamp, mock_snowflake_session
):
    """
    Test exception when multiple active records are found with end_timestamp_column
    """
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame({"cust_id": [100, 101]})
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(mock_snowflake_session, table_with_end_timestamp)
    assert (
        str(exc.value)
        == "Multiple active records found for the same natural key. Examples of natural keys with multiple active records are: [100, 101]"
    )


@pytest.mark.asyncio
async def test_validation_exception__with_end_timestamp_2(
    service, table_with_end_timestamp, mock_snowflake_session
):
    """
    Test exception when duplicate records are found with end_timestamp_column
    """
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame(),
        pd.DataFrame({"cust_id": [100, 101]}),
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(mock_snowflake_session, table_with_end_timestamp)
    assert (
        str(exc.value)
        == "Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: [100, 101]"
    )


@pytest.mark.asyncio
async def test_validation_exception__no_end_timestamp(
    service, table_no_end_timestamp, mock_snowflake_session
):
    """
    Test exception when duplicate records are found with no end_timestamp_column
    """
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame({"cust_id": [100, 101]})
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(mock_snowflake_session, table_no_end_timestamp)
    assert (
        str(exc.value)
        == "Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: [100, 101]"
    )
