# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import SCDTable

scd_table = SCDTable.get_by_id(ObjectId("{table_id}"))
scd_view = scd_table.get_view(
    view_mode="manual",
    drop_column_names=["is_active"],
    column_cleaning_operations=[],
)
scd_view["os_type"] = "unknown"
col = scd_view["os_type"]
view = scd_view.copy()
view["os_type"][col.str.contains(pat="window", case=True)] = "window"
view_1 = view.copy()
view_1["os_type"][col.str.contains(pat="mac", case=True)] = "mac"
grouped = view_1.as_features(
    column_names=["os_type"], feature_names=["os_type"], offset=None
)
feat = grouped["os_type"]
output = feat
