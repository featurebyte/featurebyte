# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from pandas import Timestamp

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
col = event_view["event_timestamp"]
timestamp_value = Timestamp("2001-01-01T00:00:00")
timestamp_value_1 = Timestamp("2050-01-01T00:00:00")
col_1 = (col > timestamp_value) & (col < timestamp_value_1)
view = event_view[col_1]
grouped = view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
    value_column="col_float",
    method="sum",
    windows=["30m"],
    feature_names=["sum_30m"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="600s", period="1800s", offset="300s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["sum_30m"]
output = feat
