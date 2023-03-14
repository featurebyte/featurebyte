# Generated by SDK version: 0.1.0
from bson import ObjectId
from featurebyte import EventData

event_data = EventData.get_by_id(ObjectId("{data_id}"))
event_view = event_data.get_view(
    view_mode="manual", drop_column_names=["created_at"], column_cleaning_operations=[]
)
output = event_view
