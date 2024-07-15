# Generated by SDK version: {sdk_version}
from bson import ObjectId

from featurebyte import EventTable

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
col = event_view["event_timestamp"]
view = event_view.copy()
view["event_timestamp_hour"] = col.dt.tz_offset("+08:00").hour
output = view
