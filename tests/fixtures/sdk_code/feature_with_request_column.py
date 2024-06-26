# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from featurebyte.api.request_column import RequestColumn

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
grouped = event_view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
    value_column="event_timestamp",
    method="latest",
    windows=["90d"],
    feature_names=["latest_event_timestamp_90d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="600s", period="1800s", offset="300s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["latest_event_timestamp_90d"]
request_col = RequestColumn.point_in_time()
feat_1 = (request_col - feat).dt.day
feat_1.name = "Time Since Last Event (days)"
output = feat_1
