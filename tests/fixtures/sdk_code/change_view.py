# Generated by SDK version: {sdk_version}
from bson import ObjectId
from featurebyte import FeatureJobSetting
from featurebyte import SCDTable

scd_table = SCDTable.get_by_id(ObjectId("{table_id}"))
change_view = scd_table.get_change_view(
    track_changes_column="col_int",
    default_feature_job_setting=FeatureJobSetting(
        blind_spot="0", frequency="24h", time_modulo_frequency="1h"
    ),
    prefixes=(None, "_past"),
    view_mode="manual",
    drop_column_names=[],
    column_cleaning_operations=[],
)
output = change_view
