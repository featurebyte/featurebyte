# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import CalendarWindow
from featurebyte import CronFeatureJobSetting
from featurebyte import TimeSeriesTable

time_series_table = TimeSeriesTable.get_by_id(
    ObjectId("{table_id}")
)
time_series_view = time_series_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
grouped = time_series_view.groupby(
    by_keys=["store_id"], category=None
).aggregate_over(
    value_column="col_float",
    method="sum",
    windows=[CalendarWindow(unit="MONTH", size=3)],
    feature_names=["col_float_sum_3month"],
    feature_job_setting=CronFeatureJobSetting(
        crontab="0 8 1 * *",
        timezone="Etc/UTC",
        reference_timezone=None,
        blind_spot=CalendarWindow(unit="MONTH", size=1),
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["col_float_sum_3month"]
output = feat
