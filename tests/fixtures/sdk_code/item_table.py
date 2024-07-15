# Generated by SDK version: {sdk_version}
from bson import ObjectId

from featurebyte import FeatureStore, ItemTable, SnowflakeDetails
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails

item_table = ItemTable(
    name="item_table",
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
            table_name="items_table",
        ),
    ),
    columns_info=[
        ColumnInfo(name="event_id_col", dtype="INT"),
        ColumnInfo(name="item_id_col", dtype="VARCHAR"),
        ColumnInfo(name="item_type", dtype="VARCHAR"),
        ColumnInfo(name="item_amount", dtype="FLOAT"),
        ColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
        ColumnInfo(name="event_timestamp", dtype="TIMESTAMP_TZ"),
    ],
    record_creation_timestamp_column=None,
    item_id_column="item_id_col",
    event_id_column="event_id_col",
    event_table_id=ObjectId("{event_table_id}"),
    _id=ObjectId("{table_id}"),
)
output = item_table
