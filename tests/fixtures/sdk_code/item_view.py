# Generated by SDK version: 0.1.0
from bson import ObjectId
<<<<<<< HEAD
from featurebyte import ItemData

item_data = ItemData.get_by_id(ObjectId("{data_id}"))
item_view = item_data.get_view(
=======
from featurebyte import ItemTable
from featurebyte import ItemView

item_table = ItemTable.get_by_id(ObjectId("{data_id}"))
item_view = ItemView.from_item_data(
    item_data=item_table,
>>>>>>> d66b352f (Rename data to source table)
    event_suffix="_event_data",
    view_mode="manual",
    drop_column_names=[],
    column_cleaning_operations=[],
    event_drop_column_names=["created_at"],
    event_column_cleaning_operations=[],
    event_join_column_names=["event_timestamp", "cust_id"],
)
output = item_view
