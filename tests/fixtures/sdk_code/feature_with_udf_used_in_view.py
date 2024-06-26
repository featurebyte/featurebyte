# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import EventTable
from featurebyte import FeatureJobSetting
from featurebyte import UserDefinedFunction

event_table = EventTable.get_by_id(ObjectId("{event_table_id}"))
event_view = event_table.get_view(
    view_mode="manual",
    drop_column_names=["created_at"],
    column_cleaning_operations=[],
)
col = event_view["col_float"]

# udf_name: power_func, sql_function_name: power
udf_power_func = UserDefinedFunction.get_by_id(
    ObjectId("{power_function_id}")
)
col_1 = udf_power_func(col, 2)
view = event_view.copy()
view["float_square"] = col_1
col_2 = view["float_square"]

# udf_name: cos_func, sql_function_name: cos
udf_cos_func = UserDefinedFunction.get_by_id(
    ObjectId("{cos_function_id}")
)
col_3 = udf_cos_func(col_2)
view_1 = view.copy()
view_1["cos_float_square"] = col_3
grouped = view_1.groupby(by_keys=["cust_id"], category=None).aggregate_over(
    value_column="cos_float_square",
    method="sum",
    windows=["1d"],
    feature_names=["sum_cos_float_square"],
    feature_job_setting=FeatureJobSetting(
        blind_spot="600s", period="1800s", offset="300s"
    ),
    skip_fill_na=True,
    offset=None,
)
feat = grouped["sum_cos_float_square"]
feat_1 = udf_cos_func(feat)
feat_1.name = "sum_cos_float_square"
output = feat_1
