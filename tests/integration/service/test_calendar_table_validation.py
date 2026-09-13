"""
Integration tests for CalendarTableValidationService
"""

import pandas as pd
import pytest

from featurebyte import TimestampSchema
from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata
from featurebyte.schema.calendar_table import CalendarTableCreate
from featurebyte.service.calendar_table import CalendarTableService
from featurebyte.service.calendar_table_validation import CalendarTableValidationService


@pytest.fixture
def service(app_container) -> CalendarTableValidationService:
    """Fixture for CalendarTableValidationService"""
    return app_container.calendar_table_validation_service


@pytest.fixture
def document_service(app_container) -> CalendarTableService:
    """Fixture for CalendarTableService"""
    return app_container.calendar_table_service


def _make_calendar_create_payload(
    feature_store, session, table_name, format_string, series_id_column=None
):
    """Create a CalendarTableCreate payload with VARCHAR datetime and optional series_id."""
    columns_info = [
        ColumnSpecWithDescription(name="cal_date", dtype=DBVarType.VARCHAR),
    ]
    if series_id_column is not None:
        columns_info.append(ColumnSpecWithDescription(name=series_id_column, dtype=DBVarType.INT))
    return CalendarTableCreate(
        name=table_name,
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=table_name,
            ),
        ),
        columns_info=columns_info,
        calendar_datetime_column="cal_date",
        calendar_datetime_schema=TimestampSchema(format_string=format_string),
        series_id_column=series_id_column,
    )


def _make_calendar_create_payload_timestamp(
    feature_store, session, table_name, series_id_column=None
):
    """Create a CalendarTableCreate payload with TIMESTAMP datetime column."""
    columns_info = [
        ColumnSpecWithDescription(name="cal_date", dtype=DBVarType.TIMESTAMP),
    ]
    if series_id_column is not None:
        columns_info.append(ColumnSpecWithDescription(name=series_id_column, dtype=DBVarType.INT))
    return CalendarTableCreate(
        name=table_name,
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=table_name,
            ),
        ),
        columns_info=columns_info,
        calendar_datetime_column="cal_date",
        calendar_datetime_schema=TimestampSchema(),
        series_id_column=series_id_column,
    )


def _make_metadata(table_model, feature_store, session):
    return ExtendedSourceMetadata(
        columns_info=table_model.columns_info,
        feature_store_id=table_model.tabular_source.feature_store_id,
        feature_store_details=feature_store.get_feature_store_details(),
        source_info=session.get_source_info(),
    )


# --- Valid cases ---


@pytest.mark.parametrize("table_name", ["test_cal_valid_varchar"])
@pytest.mark.asyncio
async def test_valid_varchar_calendar_table(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """Valid calendar table: unique day-level VARCHAR dates."""
    session = session_without_datasets
    df = pd.DataFrame({"cal_date": ["2022|01|01", "2022|01|02", "2022|01|03"]})
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload(
            feature_store, session, table_name, format_string=timestamp_format_string
        )
    )
    await service._validate_table(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )


@pytest.mark.parametrize("table_name", ["test_cal_valid_varchar_with_series_id"])
@pytest.mark.asyncio
async def test_valid_varchar_calendar_table_with_series_id(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """Valid calendar table: same date allowed for different series."""
    session = session_without_datasets
    df = pd.DataFrame({
        "cal_date": ["2022|01|01", "2022|01|01", "2022|01|02", "2022|01|02"],
        "series_id": [1, 2, 1, 2],
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload(
            feature_store,
            session,
            table_name,
            format_string=timestamp_format_string,
            series_id_column="series_id",
        )
    )
    await service._validate_table(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )


@pytest.mark.parametrize("table_name", ["test_cal_valid_timestamp"])
@pytest.mark.asyncio
async def test_valid_timestamp_calendar_table(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
):
    """Valid calendar table: TIMESTAMP column at day boundaries."""
    session = session_without_datasets
    df = pd.DataFrame({
        "cal_date": pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload_timestamp(feature_store, session, table_name)
    )
    await service._validate_table(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )


# --- Invalid: not day granularity ---


@pytest.mark.parametrize("table_name", ["test_cal_invalid_granularity_varchar"])
@pytest.mark.asyncio
async def test_invalid_day_granularity_varchar(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string_with_time,
):
    """VARCHAR column with sub-day time components should fail validation."""
    session = session_without_datasets
    df = pd.DataFrame({"cal_date": ["2022|01|01|08:00:00", "2022|01|02|14:30:00"]})
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload(
            feature_store,
            session,
            table_name,
            format_string=timestamp_format_string_with_time,
        )
    )
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(
            session, table_model, _make_metadata(table_model, feature_store, session)
        )
    assert "not at day granularity" in str(exc_info.value)
    assert "'cal_date'" in str(exc_info.value)


@pytest.mark.parametrize("table_name", ["test_cal_invalid_granularity_timestamp"])
@pytest.mark.asyncio
async def test_invalid_day_granularity_timestamp(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
):
    """TIMESTAMP column with sub-day time components should fail validation."""
    session = session_without_datasets
    df = pd.DataFrame({
        "cal_date": pd.to_datetime([
            "2022-01-01 08:05:00",
            "2022-01-02 14:30:00",
        ]),
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload_timestamp(feature_store, session, table_name)
    )
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(
            session, table_model, _make_metadata(table_model, feature_store, session)
        )
    assert "not at day granularity" in str(exc_info.value)
    assert "'cal_date'" in str(exc_info.value)


# --- Invalid: duplicate rows ---


@pytest.mark.parametrize("table_name", ["test_cal_invalid_duplicate_no_series"])
@pytest.mark.asyncio
async def test_invalid_duplicate_dates_no_series_id(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """Duplicate calendar dates without series_id should fail validation."""
    session = session_without_datasets
    df = pd.DataFrame({"cal_date": ["2022|01|01", "2022|01|01", "2022|01|02"]})
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload(
            feature_store, session, table_name, format_string=timestamp_format_string
        )
    )
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(
            session, table_model, _make_metadata(table_model, feature_store, session)
        )
    assert (
        str(exc_info.value)
        == "Calendar table contains duplicate rows for the same 'cal_date' combination. "
        "Each calendar date must be unique per series."
    )


@pytest.mark.parametrize("table_name", ["test_cal_invalid_duplicate_with_series"])
@pytest.mark.asyncio
async def test_invalid_duplicate_dates_with_series_id(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """Duplicate (date, series_id) pairs should fail validation."""
    session = session_without_datasets
    df = pd.DataFrame({
        "cal_date": ["2022|01|01", "2022|01|01", "2022|01|02"],
        "series_id": [1, 1, 1],  # duplicate (2022|01|01, 1)
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_calendar_create_payload(
            feature_store,
            session,
            table_name,
            format_string=timestamp_format_string,
            series_id_column="series_id",
        )
    )
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(
            session, table_model, _make_metadata(table_model, feature_store, session)
        )
    assert (
        str(exc_info.value)
        == "Calendar table contains duplicate rows for the same 'cal_date' and 'series_id' combination. "
        "Each calendar date must be unique per series."
    )
