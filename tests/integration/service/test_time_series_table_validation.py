"""
Integration tests for TimeSeriesTableValidationService
"""

import pandas as pd
import pytest

from featurebyte import TimeInterval, TimestampSchema
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata
from featurebyte.schema.time_series_table import TimeSeriesTableCreate
from featurebyte.service.time_series_table import TimeSeriesTableService
from featurebyte.service.time_series_table_validation import TimeSeriesTableValidationService


@pytest.fixture
def service(app_container) -> TimeSeriesTableValidationService:
    """
    Fixture for TimeSeriesTableValidationService
    """
    return app_container.time_series_table_validation_service


@pytest.fixture
def document_service(app_container) -> TimeSeriesTableService:
    """
    Fixture for TimeSeriesTableService
    """
    return app_container.time_series_table_service


def _make_ts_create_payload(feature_store, session, table_name, series_id_column, format_string):
    """Payload using a VARCHAR column with a platform-specific format string."""
    columns_info = [
        ColumnSpecWithDescription(name="snapshot_date", dtype=DBVarType.VARCHAR),
    ]
    if series_id_column is not None:
        columns_info.append(ColumnSpecWithDescription(name="cust_id", dtype=DBVarType.INT))
    return TimeSeriesTableCreate(
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
        reference_datetime_column="snapshot_date",
        reference_datetime_schema=TimestampSchema(format_string=format_string),
        series_id_column=series_id_column,
        time_interval=TimeInterval(unit="DAY", value=1),
    )


def _make_ts_create_payload_timestamp(feature_store, session, table_name):
    """Payload using a native TIMESTAMP column (no format_string) for jitter tests."""
    return TimeSeriesTableCreate(
        name=table_name,
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=table_name,
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(name="snapshot_date", dtype=DBVarType.TIMESTAMP),
        ],
        reference_datetime_column="snapshot_date",
        reference_datetime_schema=TimestampSchema(),
        series_id_column=None,
        time_interval=TimeInterval(unit="DAY", value=1),
    )


def _make_metadata(table_model, feature_store, session):
    return ExtendedSourceMetadata(
        columns_info=table_model.columns_info,
        feature_store_id=table_model.tabular_source.feature_store_id,
        feature_store_details=feature_store.get_feature_store_details(),
        source_info=session.get_source_info(),
    )


@pytest.mark.parametrize("table_name", ["test_ts_detect_global_series__unique"])
@pytest.mark.asyncio
async def test_detect_global_series__unique_datetimes(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """
    Global series detection returns True when reference datetimes are unique per aligned time point.
    """
    session = session_without_datasets
    df = pd.DataFrame({"snapshot_date": ["2022|01|01", "2022|01|02", "2022|01|03"]})
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_ts_create_payload(
            feature_store,
            session,
            table_name,
            series_id_column=None,
            format_string=timestamp_format_string,
        )
    )
    is_global = await service._detect_is_global_series(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )
    assert is_global is True


@pytest.mark.parametrize("table_name", ["test_ts_detect_global_series__duplicate"])
@pytest.mark.asyncio
async def test_detect_global_series__duplicate_datetimes(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """
    Global series detection returns False when multiple rows share the same aligned datetime.
    """
    session = session_without_datasets
    # Two rows on the same day - not a valid global series
    df = pd.DataFrame({"snapshot_date": ["2022|01|01", "2022|01|01", "2022|01|02"]})
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_ts_create_payload(
            feature_store,
            session,
            table_name,
            series_id_column=None,
            format_string=timestamp_format_string,
        )
    )
    is_global = await service._detect_is_global_series(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )
    assert is_global is False


@pytest.mark.parametrize("table_name", ["test_ts_detect_global_series__has_series_id"])
@pytest.mark.asyncio
async def test_detect_global_series__has_series_id_column(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
    timestamp_format_string,
):
    """
    Global series detection returns False immediately when series_id_column is set
    without querying the warehouse.
    """
    session = session_without_datasets
    df = pd.DataFrame({
        "snapshot_date": ["2022|01|01", "2022|01|02"],
        "cust_id": [1, 2],
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_ts_create_payload(
            feature_store,
            session,
            table_name,
            series_id_column="cust_id",
            format_string=timestamp_format_string,
        )
    )
    is_global = await service._detect_is_global_series(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )
    assert is_global is False


@pytest.mark.parametrize("table_name", ["test_ts_detect_global_series__jitter_unique"])
@pytest.mark.asyncio
async def test_detect_global_series__jitter_unique(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
):
    """
    Global series detection returns True when jittered timestamps are unique per aligned day.
    Each record falls on a different calendar day despite having different intra-day times.
    """
    session = session_without_datasets
    df = pd.DataFrame({
        "snapshot_date": pd.to_datetime([
            "2022-01-01 08:05:00",
            "2022-01-02 14:30:00",
            "2022-01-03 23:59:00",
        ])
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_ts_create_payload_timestamp(feature_store, session, table_name)
    )
    is_global = await service._detect_is_global_series(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )
    assert is_global is True


@pytest.mark.parametrize("table_name", ["test_ts_detect_global_series__jitter_duplicate"])
@pytest.mark.asyncio
async def test_detect_global_series__jitter_duplicate(
    service,
    document_service,
    session_without_datasets,
    table_name,
    feature_store,
):
    """
    Global series detection returns False when two jittered timestamps fall on the same calendar day.
    """
    session = session_without_datasets
    df = pd.DataFrame({
        "snapshot_date": pd.to_datetime([
            "2022-01-01 08:05:00",
            "2022-01-01 17:45:00",  # same day, different time
            "2022-01-02 09:00:00",
        ])
    })
    await session.register_table(table_name, df)

    table_model = await document_service.create_document(
        _make_ts_create_payload_timestamp(feature_store, session, table_name)
    )
    is_global = await service._detect_is_global_series(
        session, table_model, _make_metadata(table_model, feature_store, session)
    )
    assert is_global is False
