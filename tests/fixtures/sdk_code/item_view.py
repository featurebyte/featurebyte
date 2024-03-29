# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import ItemTable

item_table = ItemTable.get_by_id(ObjectId("{table_id}"))
item_view = item_table.get_view(
    event_suffix="_event_table",
    view_mode="manual",
    drop_column_names=[],
    column_cleaning_operations=[],
    event_drop_column_names=["created_at"],
    event_column_cleaning_operations=[],
    event_join_column_names=["event_timestamp", "cust_id"],
)
output = item_view
