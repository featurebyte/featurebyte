# Generated by SDK version: 0.1.0
from bson import ObjectId
from featurebyte import EventData

event_data = EventData.get_by_id(ObjectId("{data_id}"))
output = event_data
