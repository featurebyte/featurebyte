# Generated by SDK version: 0.1.0
from bson import ObjectId
from featurebyte import FeatureStore
from featurebyte import SlowlyChangingData
from featurebyte import SnowflakeDetails
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails

slowly_changing_data = SlowlyChangingData(
    name="slowly_changing_data",
    feature_store=FeatureStore(
        name="sf_featurestore",
        type="snowflake",
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            database="sf_database",
            sf_schema="sf_schema",
        ),
    ),
    tabular_source=TabularSource(
        feature_store_id=ObjectId("{feature_store_id}"),
        table_details=TableDetails(
            database_name="sf_database", schema_name="sf_schema", table_name="scd_table"
        ),
    ),
    columns_info=[
        ColumnInfo(name="col_int", dtype="INT"),
        ColumnInfo(name="col_float", dtype="FLOAT"),
        ColumnInfo(name="is_active", dtype="BOOL"),
        ColumnInfo(name="col_text", dtype="VARCHAR"),
        ColumnInfo(name="col_binary", dtype="BINARY"),
        ColumnInfo(name="col_boolean", dtype="BOOL"),
        ColumnInfo(name="effective_timestamp", dtype="TIMESTAMP_TZ"),
        ColumnInfo(name="end_timestamp", dtype="TIMESTAMP_TZ"),
        ColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
        ColumnInfo(name="cust_id", dtype="INT"),
    ],
    record_creation_date_column=None,
    natural_key_column="col_text",
    effective_timestamp_column="effective_timestamp",
    end_timestamp_column="end_timestamp",
    surrogate_key_column="col_int",
    current_flag_column="is_active",
    _id=ObjectId("{data_id}"),
)
output = slowly_changing_data
