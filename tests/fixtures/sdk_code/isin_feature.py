# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable
from featurebyte import FeatureJobSetting

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
grouped = event_view.as_features(
    column_names=["cust_id"], feature_names=["cust_id_feature"], offset=None
)
feat = grouped["cust_id_feature"]
grouped_1 = event_view.groupby(
    by_keys=["cust_id"], category="col_int"
).aggregate_over(
    value_column=None,
    method="count",
    windows=["24h"],
    feature_names=["count_a_24h_per_col_int"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="3600s", period="3600s", offset="1800s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat_1 = grouped_1["count_a_24h_per_col_int"]
feat_2 = feat.isin(feat_1)
feat_2.name = "lookup_feature_isin_count_per_category_feature"
output = feat_2
