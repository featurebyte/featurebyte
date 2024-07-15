# Generated by SDK version: {sdk_version}
from bson import ObjectId

from featurebyte import DimensionTable, EventTable

dimension_table = DimensionTable.get_by_id(ObjectId("{dimension_table_id}"))
dimension_view = dimension_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
view = dimension_view[["col_int", "col_text"]]
event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
joined_view = event_view.join(view, on="col_int", how="left", rsuffix="", rprefix="")
col = joined_view["tz_offset"]
col_1 = joined_view["event_timestamp"]
view_1 = joined_view.copy()
view_1["event_timestamp_hour"] = col_1.dt.tz_offset(col).hour
output = view_1
