# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from featurebyte import ItemTable

item_table = ItemTable.get_by_id(ObjectId("{item_table_id}"))
item_view = item_table.get_view(
    event_suffix="_event_table",
    view_mode="manual",
    drop_column_names=[],
    column_cleaning_operations=[],
    event_drop_column_names=["created_at"],
    event_column_cleaning_operations=[],
    event_join_column_names=["event_timestamp", "cust_id"],
)
feat = item_view.groupby(by_keys=["event_id_col"], category=None).aggregate(
    value_column="item_amount",
    method="sum",
    feature_name="sum_item_amount",
    skip_fill_na=True,
)
feat_1 = feat.copy()
feat_1[feat.isnull()] = 0
event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
joined_view = event_view.add_feature(
    new_column_name="sum_item_amt", feature=feat_1, entity_column="cust_id"
)
col = joined_view["sum_item_amt"]
view = joined_view.copy()
view["sum_item_amt_plus_one"] = col + 1
grouped = view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
    value_column="sum_item_amt_plus_one",
    method="sum",
    windows=["30d"],
    feature_names=["sum_item_amt_plus_one_over_30d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", frequency="360s", time_modulo_frequency="180s"
    ),
    skip_fill_na=True,
)
feat_2 = grouped["sum_item_amt_plus_one_over_30d"]
grouped_1 = joined_view.groupby(
    by_keys=["cust_id"], category=None
).aggregate_over(
    value_column="sum_item_amt",
    method="sum",
    windows=["30d"],
    feature_names=["sum_item_amt_over_30d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", frequency="360s", time_modulo_frequency="180s"
    ),
    skip_fill_na=True,
)
feat_3 = grouped_1["sum_item_amt_over_30d"]
feat_4 = feat_3 / feat_2
feat_4.name = "fraction"
output = feat_4
