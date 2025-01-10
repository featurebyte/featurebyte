# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import FeatureStore
from featurebyte import SnowflakeDetails
from featurebyte import TimeInterval
from featurebyte import TimeSeriesTable
from featurebyte import TimestampSchema
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails

time_series_table = TimeSeriesTable(
    name="time_series_table",
    feature_store=FeatureStore(
        name="sf_featurestore",
        type="snowflake",
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            database_name="sf_database",
            schema_name="sf_schema",
            role_name="TESTING",
        ),
    ),
    tabular_source=TabularSource(
        feature_store_id=ObjectId("{feature_store_id}"),
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="time_series_table",
        ),
    ),
    columns_info=[
        ColumnInfo(name="col_int", dtype="INT"),
        ColumnInfo(name="col_float", dtype="FLOAT"),
        ColumnInfo(name="col_char", dtype="CHAR"),
        ColumnInfo(name="col_text", dtype="VARCHAR"),
        ColumnInfo(name="col_binary", dtype="BINARY"),
        ColumnInfo(name="col_boolean", dtype="BOOL"),
        ColumnInfo(name="date", dtype="VARCHAR"),
        ColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
        ColumnInfo(name="store_id", dtype="INT"),
    ],
    record_creation_timestamp_column=None,
    series_id_column="col_int",
    reference_datetime_column="date",
    reference_datetime_schema=TimestampSchema(
        format_string="YYYY-MM-DD HH24:MI:SS",
        is_utc_time=None,
        timezone="Etc/UTC",
    ),
    reference_timezone=None,
    time_interval=TimeInterval(value=1, unit="DAY"),
    _id=ObjectId("{table_id}"),
)
output = time_series_table
