# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import AddTimestampSchema
from featurebyte import ColumnCleaningOperation
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from featurebyte import TimestampSchema

event_table = EventTable.get_by_id(ObjectId("{table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[
        ColumnCleaningOperation(
            column_name="col_text",
            cleaning_operations=[
                AddTimestampSchema(
                    timestamp_schema=TimestampSchema(
                        format_string="%Y-%m-%d %H:%M:%S",
                        is_utc_time=False,
                        timezone="Asia/Singapore",
                    )
                )
            ],
        )
    ],
)
col = event_view["col_text"]
view = event_view.copy()
view["col_text_day"] = col.dt.day
grouped = view.groupby(by_keys=["cust_id"], category=None).aggregate_over(
    value_column="col_text_day",
    method="max",
    windows=["7d"],
    feature_names=["max_col_text_day"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="90s", period="900s", offset="180s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["max_col_text_day"]
output = feat
