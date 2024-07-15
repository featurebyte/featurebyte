# Generated by SDK version: {sdk_version}
from bson import ObjectId

from featurebyte import DimensionTable

dimension_table = DimensionTable.get_by_id(ObjectId("{table_id}"))
dimension_view = dimension_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
output = dimension_view
