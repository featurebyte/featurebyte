# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import SCDTable

scd_table = SCDTable.get_by_id(ObjectId("{table_id}"))
scd_view = scd_table.get_view(
    view_mode="manual", drop_column_names=[], column_cleaning_operations=[]
)
feat = scd_view.groupby(by_keys=["col_int"], category=None).aggregate_asat(
    value_column="col_float",
    method="max",
    feature_name="col_float_max",
    offset=None,
    backward=True,
    skip_fill_na=True,
)
output = feat
