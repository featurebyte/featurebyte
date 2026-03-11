"""
Unit tests for get_default_partition_column in input node parameters
"""

from featurebyte.enum import DBVarType, TableDataType, TimeIntervalUnit
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.input import (
    DimensionTableInputNodeParameters,
    EventTableInputNodeParameters,
    ItemTableInputNodeParameters,
    PartitionColumnInfo,
    SCDTableInputNodeParameters,
    SnapshotsTableInputNodeParameters,
    SourceTableInputNodeParameters,
    TimeSeriesTableInputNodeParameters,
)
from featurebyte.query_graph.node.schema import TableDetails

COMMON_COLUMNS = [{"name": "col", "dtype": DBVarType.VARCHAR}]
TABLE_DETAILS = TableDetails(database_name="db", schema_name="schema", table_name="table")
FEATURE_STORE_DETAILS = {"type": "snowflake", "details": {}}


def make_base_params(extra: dict) -> dict:
    return {
        "columns": COMMON_COLUMNS,
        "table_details": TABLE_DETAILS,
        "feature_store_details": FEATURE_STORE_DETAILS,
        **extra,
    }


def test_event_table_returns_none_when_no_timestamp_column():
    """Returns None when timestamp_column is not set."""
    params = EventTableInputNodeParameters(
        **make_base_params({"type": TableDataType.EVENT_TABLE, "timestamp_column": None})
    )
    assert params.get_default_partition_column() is None


def test_event_table_returns_partition_column_without_schema():
    """Returns PartitionColumnInfo with empty metadata when no event_timestamp_schema."""
    params = EventTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.EVENT_TABLE,
            "timestamp_column": "event_ts",
            "event_timestamp_schema": None,
        })
    )
    result = params.get_default_partition_column()
    assert result == PartitionColumnInfo(name="event_ts", dtype_metadata=DBVarTypeMetadata())


def test_event_table_returns_partition_column_with_schema():
    """Returns PartitionColumnInfo with timestamp_schema embedded in metadata."""
    schema = TimestampSchema(is_utc_time=True)
    params = EventTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.EVENT_TABLE,
            "timestamp_column": "event_ts",
            "event_timestamp_schema": schema,
        })
    )
    result = params.get_default_partition_column()
    assert result == PartitionColumnInfo(
        name="event_ts", dtype_metadata=DBVarTypeMetadata(timestamp_schema=schema)
    )


def test_time_series_table_returns_reference_datetime_column():
    """Returns PartitionColumnInfo using reference_datetime_column and its schema."""
    schema = TimestampSchema(is_utc_time=True)
    params = TimeSeriesTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.TIME_SERIES_TABLE,
            "reference_datetime_column": "ref_dt",
            "reference_datetime_schema": schema,
            "time_interval": TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
        })
    )
    result = params.get_default_partition_column()
    assert result == PartitionColumnInfo(
        name="ref_dt", dtype_metadata=DBVarTypeMetadata(timestamp_schema=schema)
    )


def test_time_series_table_timestamp_schema_preserved():
    """Preserves the full TimestampSchema in the returned dtype_metadata."""
    schema = TimestampSchema(format_string="%Y-%m-%d", is_utc_time=False, timezone="Asia/Singapore")
    params = TimeSeriesTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.TIME_SERIES_TABLE,
            "reference_datetime_column": "ts_col",
            "reference_datetime_schema": schema,
            "time_interval": TimeInterval(unit=TimeIntervalUnit.HOUR, value=6),
        })
    )
    result = params.get_default_partition_column()
    assert result is not None
    assert result.dtype_metadata.timestamp_schema == schema


def test_snapshots_table_returns_snapshot_datetime_column():
    """Returns PartitionColumnInfo using snapshot_datetime_column and its schema."""
    schema = TimestampSchema(is_utc_time=True)
    params = SnapshotsTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.SNAPSHOTS_TABLE,
            "id_column": "id",
            "snapshot_datetime_column": "snap_dt",
            "snapshot_datetime_schema": schema,
            "time_interval": TimeInterval(unit=TimeIntervalUnit.DAY, value=7),
        })
    )
    result = params.get_default_partition_column()
    assert result == PartitionColumnInfo(
        name="snap_dt", dtype_metadata=DBVarTypeMetadata(timestamp_schema=schema)
    )


def test_snapshots_table_timestamp_schema_preserved():
    """Preserves the full TimestampSchema in the returned dtype_metadata."""
    schema = TimestampSchema(format_string="%Y-%m-%d", is_utc_time=False, timezone="Asia/Singapore")
    params = SnapshotsTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.SNAPSHOTS_TABLE,
            "id_column": "id",
            "snapshot_datetime_column": "snap_col",
            "snapshot_datetime_schema": schema,
            "time_interval": TimeInterval(unit=TimeIntervalUnit.MONTH, value=1),
        })
    )
    result = params.get_default_partition_column()
    assert result is not None
    assert result.dtype_metadata.timestamp_schema == schema


def test_source_table_returns_none():
    """Source table has no partition column and always returns None."""
    params = SourceTableInputNodeParameters(
        **make_base_params({"type": TableDataType.SOURCE_TABLE})
    )
    assert params.get_default_partition_column() is None


def test_item_table_returns_none():
    """Item table has no default partition column and always returns None."""
    params = ItemTableInputNodeParameters(**make_base_params({"type": TableDataType.ITEM_TABLE}))
    assert params.get_default_partition_column() is None


def test_dimension_table_returns_none():
    """Dimension table has no default partition column and always returns None."""
    params = DimensionTableInputNodeParameters(
        **make_base_params({"type": TableDataType.DIMENSION_TABLE})
    )
    assert params.get_default_partition_column() is None


def test_scd_table_returns_none():
    """SCD table has no default partition column and always returns None."""
    params = SCDTableInputNodeParameters(
        **make_base_params({
            "type": TableDataType.SCD_TABLE,
            "natural_key_column": "nk",
            "effective_timestamp_column": "eff_ts",
        })
    )
    assert params.get_default_partition_column() is None
