# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
target = event_view["col_int"].as_target(
    target_name="lookup_target", offset=None, fill_value=None
)
output = target
