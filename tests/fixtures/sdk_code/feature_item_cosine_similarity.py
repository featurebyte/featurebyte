# Generated by SDK version: {sdk_version}
from bson import ObjectId
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
grouped = item_view.groupby(
    by_keys=["cust_id_event_table"], category="item_id_col"
).aggregate_over(
    value_column="item_amount",
    method="sum",
    windows=["90d"],
    feature_names=["sum_item_amount_over_90d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", frequency="360s", time_modulo_frequency="180s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["sum_item_amount_over_90d"]
grouped_1 = item_view.groupby(
    by_keys=["cust_id_event_table"], category="item_id_col"
).aggregate_over(
    value_column="item_amount",
    method="sum",
    windows=["30d"],
    feature_names=["sum_item_amount_over_30d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", frequency="360s", time_modulo_frequency="180s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat_1 = grouped_1["sum_item_amount_over_30d"]
feat_2 = feat_1.cd.cosine_similarity(other=feat)
feat_2.name = (
    "sum_item_amount_over_30d_cosine_similarity_sum_item_amount_over_90d"
)
output = feat_2
