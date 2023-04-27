# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import ColumnCleaningOperation
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from featurebyte import MissingValueImputation

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[
        ColumnCleaningOperation(
            column_name="tz_offset",
            cleaning_operations=[
                MissingValueImputation(imputed_value="+00:00")
            ],
        )
    ],
)
col = event_view["event_timestamp"]
col_1 = event_view["tz_offset"]
event_view["timestamp_hour"] = col.dt.tz_offset(col_1).hour
grouped = event_view.groupby(
    by_keys=["cust_id"], category="timestamp_hour"
).aggregate_over(
    value_column=None,
    method="count",
    windows=["7d"],
    feature_names=["timestamp_hour_counts_7d"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", frequency="360s", time_modulo_frequency="180s"
    ),
    skip_fill_na=True,
)
feat = grouped["timestamp_hour_counts_7d"]
output = feat
