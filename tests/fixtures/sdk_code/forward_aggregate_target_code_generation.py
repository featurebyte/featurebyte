from bson import ObjectId
from featurebyte import EventTable

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
target = event_view.groupby(
    by_keys=["col_int"], category=None
).forward_aggregate(
    value_column="col_float",
    method="sum",
    window="1d",
    target_name="forward_aggregate_target",
    skip_fill_na=True,
)
output = target
