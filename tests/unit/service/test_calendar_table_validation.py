"""
Tests for CalendarTableValidationService
"""

from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import TimestampSchema
from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata
from featurebyte.schema.calendar_table import CalendarTableCreate
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="document_service")
def document_service_fixture(app_container):
    """Fixture for CalendarTableService"""
    return app_container.calendar_table_service


@pytest.fixture(name="table_validation_service")
def table_validation_service_fixture(app_container, mock_snowflake_session):
    """CalendarTableValidationService"""
    with patch(
        "featurebyte.service.base_table_validation.SessionManagerService.get_feature_store_session",
        return_value=mock_snowflake_session,
    ):
        yield app_container.calendar_table_validation_service


@pytest_asyncio.fixture(name="table_model_varchar_with_series_id")
async def table_model_varchar_with_series_id_fixture(document_service, feature_store):
    """CalendarTable with VARCHAR datetime column and series_id"""
    payload = CalendarTableCreate(
        name="my_calendar_table_varchar",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="snapshot_date",
                dtype=DBVarType.VARCHAR,
            ),
            ColumnSpecWithDescription(
                name="series_id",
                dtype=DBVarType.INT,
            ),
        ],
        calendar_datetime_column="snapshot_date",
        calendar_datetime_schema=TimestampSchema(format_string="YYYY-MM-DD"),
        series_id_column="series_id",
    )
    return await document_service.create_document(payload)


@pytest_asyncio.fixture(name="table_model_varchar_no_series_id")
async def table_model_varchar_no_series_id_fixture(document_service, feature_store):
    """CalendarTable with VARCHAR datetime column and no series_id"""
    payload = CalendarTableCreate(
        name="my_calendar_table_varchar_no_series",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="snapshot_date",
                dtype=DBVarType.VARCHAR,
            ),
        ],
        calendar_datetime_column="snapshot_date",
        calendar_datetime_schema=TimestampSchema(format_string="YYYY-MM-DD"),
        series_id_column=None,
    )
    return await document_service.create_document(payload)


@pytest_asyncio.fixture(name="table_model_timestamp")
async def table_model_timestamp_fixture(document_service, feature_store):
    """CalendarTable with TIMESTAMP datetime column"""
    payload = CalendarTableCreate(
        name="my_calendar_table_timestamp",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="snapshot_date",
                dtype=DBVarType.TIMESTAMP,
            ),
            ColumnSpecWithDescription(
                name="series_id",
                dtype=DBVarType.INT,
            ),
        ],
        calendar_datetime_column="snapshot_date",
        calendar_datetime_schema=TimestampSchema(),
        series_id_column="series_id",
    )
    return await document_service.create_document(payload)


@pytest_asyncio.fixture(name="table_model_date")
async def table_model_date_fixture(document_service, feature_store):
    """CalendarTable with DATE datetime column"""
    payload = CalendarTableCreate(
        name="my_calendar_table_date",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="snapshot_date",
                dtype=DBVarType.DATE,
            ),
        ],
        calendar_datetime_column="snapshot_date",
        calendar_datetime_schema=TimestampSchema(),
        series_id_column=None,
    )
    return await document_service.create_document(payload)


@pytest.fixture(name="mock_metadata")
def mock_metadata_fixture():
    """Fixture for ExtendedSourceMetadata"""
    from bson import ObjectId

    from featurebyte.enum import SourceType
    from featurebyte.query_graph.node.schema import FeatureStoreDetails, SnowflakeDetails
    from featurebyte.query_graph.sql.source_info import SourceInfo

    return ExtendedSourceMetadata(
        columns_info=[],
        feature_store_id=ObjectId(),
        feature_store_details=FeatureStoreDetails(
            type=SourceType.SNOWFLAKE,
            details=SnowflakeDetails(
                account="sf_account",
                database_name="my_db",
                schema_name="my_schema",
                warehouse="sf_warehouse",
                role_name="TESTING",
            ),
        ),
        source_info=SourceInfo(
            database_name="my_db",
            schema_name="my_schema",
            source_type=SourceType.SNOWFLAKE,
        ),
    )


@pytest.fixture(name="service")
def service_fixture(app_container):
    """Fixture for CalendarTableValidationService"""
    return app_container.calendar_table_validation_service


@pytest.mark.asyncio
async def test_validate_table__varchar_with_series_id(
    service,
    mock_snowflake_session,
    table_model_varchar_with_series_id,
    mock_metadata,
    update_fixtures,
):
    """Test _validate_table SQL for VARCHAR datetime column with series_id"""
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(
        mock_snowflake_session, table_model_varchar_with_series_id, mock_metadata
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/calendar_table_validation_service/varchar_with_series_id.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validate_table__varchar_no_series_id(
    service,
    mock_snowflake_session,
    table_model_varchar_no_series_id,
    mock_metadata,
    update_fixtures,
):
    """Test _validate_table SQL for VARCHAR datetime column without series_id"""
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(
        mock_snowflake_session, table_model_varchar_no_series_id, mock_metadata
    )
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/calendar_table_validation_service/varchar_no_series_id.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validate_table__timestamp(
    service,
    mock_snowflake_session,
    table_model_timestamp,
    mock_metadata,
    update_fixtures,
):
    """Test _validate_table SQL for TIMESTAMP datetime column"""
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(mock_snowflake_session, table_model_timestamp, mock_metadata)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/calendar_table_validation_service/timestamp_with_series_id.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validate_table__date_skips_granularity_check(
    service,
    mock_snowflake_session,
    table_model_date,
    mock_metadata,
):
    """DATE columns skip the day granularity check (always day-granular)"""
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(mock_snowflake_session, table_model_date, mock_metadata)
    # Only uniqueness check should run (no granularity check for DATE)
    assert mock_snowflake_session.execute_query_long_running.call_count == 1


@pytest.mark.asyncio
async def test_validate_table__day_granularity_violation(
    service,
    mock_snowflake_session,
    table_model_varchar_with_series_id,
    mock_metadata,
):
    """Test error raised when calendar datetime values are not at day granularity"""
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame({"snapshot_date": ["2024-01-01 14:30:00", "2024-01-02 08:00:00"]}),
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(
            mock_snowflake_session, table_model_varchar_with_series_id, mock_metadata
        )
    assert "not at day granularity" in str(exc.value)
    assert "snapshot_date" in str(exc.value)
    assert "2024-01-01 14:30:00" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_table__uniqueness_violation_with_series_id(
    service,
    mock_snowflake_session,
    table_model_varchar_with_series_id,
    mock_metadata,
):
    """Test error raised when (date, series_id) combinations are not unique"""
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame(),  # day granularity check passes
        pd.DataFrame({"snapshot_date": ["2024-01-01"], "series_id": [42]}),  # duplicate found
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(
            mock_snowflake_session, table_model_varchar_with_series_id, mock_metadata
        )
    assert "duplicate rows" in str(exc.value)
    assert "'snapshot_date'" in str(exc.value)
    assert "'series_id'" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_table__uniqueness_violation_no_series_id(
    service,
    mock_snowflake_session,
    table_model_varchar_no_series_id,
    mock_metadata,
):
    """Test error raised when dates are not unique (no series_id)"""
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame(),  # day granularity check passes
        pd.DataFrame({"snapshot_date": ["2024-01-01"]}),  # duplicate found
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(
            mock_snowflake_session, table_model_varchar_no_series_id, mock_metadata
        )
    assert "duplicate rows" in str(exc.value)
    assert "'snapshot_date'" in str(exc.value)


@pytest.mark.asyncio
async def test_validate_and_update(
    table_validation_service,
    document_service,
    table_model_varchar_with_series_id,
    mock_snowflake_session,
    update_fixtures,
):
    """Test full validate_and_update flow for a VARCHAR CalendarTable with series_id"""
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame({"snapshot_date": ["2024-01-01"]}),  # format string validation
        pd.DataFrame(),  # day granularity check passes
        pd.DataFrame(),  # uniqueness check passes
    ]
    await table_validation_service.validate_and_update(table_model_varchar_with_series_id.id)

    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/calendar_table_validation_service/validate_and_update.sql",
        update_fixtures,
    )

    updated = await document_service.get_document(table_model_varchar_with_series_id.id)
    assert updated.dict()["validation"]["status"] == "PASSED"
