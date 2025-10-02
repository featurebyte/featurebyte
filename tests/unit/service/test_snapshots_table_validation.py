"""
Unit tests for SCDTableValidationService
"""

import pandas as pd
import pytest
import pytest_asyncio

from featurebyte import TimeInterval, TimestampSchema
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.snapshots_table import SnapshotsTableCreate
from featurebyte.service.snapshots_table import SnapshotsTableService
from featurebyte.service.snapshots_table_validation import SnapshotsTableValidationService
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="service")
def service_fixture(app_container) -> SnapshotsTableValidationService:
    """
    Fixture for SCDTableValidationService
    """
    return app_container.snapshots_table_validation_service


@pytest.fixture(name="document_service")
def document_service_fixture(app_container) -> SnapshotsTableService:
    """
    Fixture for SCDTableService
    """
    return app_container.snapshots_table_service


@pytest.fixture(name="table_create_payload")
def table_create_payload_fixture(feature_store):
    """
    Fixture for SnapshotsTableCreate with no end_timestamp_column
    """
    return SnapshotsTableCreate(
        name="my_snapshots_table",
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
                name="cust_id",
                dtype=DBVarType.INT,
            ),
        ],
        snapshot_id_column="cust_id",
        snapshot_datetime_column="snapshot_date",
        time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
        snapshot_datetime_schema=TimestampSchema(format_string="%Y-%m-%d"),
    )


@pytest_asyncio.fixture(name="snapshots_table")
async def snapshots_table(document_service, table_create_payload):
    """
    Fixture for SnapshotsTableModel
    """
    return await document_service.create_document(table_create_payload)


@pytest.mark.asyncio
async def test_validation_query__no_end_timestamp(
    service,
    mock_snowflake_session,
    snapshots_table,
    adapter,
    update_fixtures,
):
    """
    Test active record counts query when end_timestamp_column is None
    """
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame()
    await service._validate_table(mock_snowflake_session, snapshots_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/snapshots_table_validation/detect_duplicates.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_validation_exception(service, snapshots_table, mock_snowflake_session):
    """
    Test validation exception handling
    """
    mock_snowflake_session.execute_query_long_running.side_effect = [
        pd.DataFrame({
            "cust_id": [100],
            "snapshot_date": ["2023-01-01"],
        })
    ]
    with pytest.raises(TableValidationError) as exc:
        await service._validate_table(mock_snowflake_session, snapshots_table)
    expected = (
        "Table my_snapshots_table is not a valid snapshots table. "
        "The following snapshot ID column and snapshot datetime column pairs are not unique: "
        "[{'cust_id': 100, 'snapshot_date': '2023-01-01'}]"
    )
    assert str(exc.value) == expected
