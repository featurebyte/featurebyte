"""
This test module check feature binary operation validation
"""

import pytest

from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    TableIdFeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    TableIdCleaningOperation,
)


def test_extract_table_id_to_feature_job_settings_and_cleaning_operations__time_aware_aggregate(
    float_feature, snowflake_event_table_id
):
    """Test extract table id to feature job settings and cleaning operations"""
    float_feature.save()
    feature_model = float_feature.cached_model
    table_id_feature_job_settings = feature_model.extract_table_id_feature_job_settings()
    assert table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_event_table_id,
            feature_job_setting=FeatureJobSetting(blind_spot="600s", period="1800s", offset="300s"),
        )
    ]

    table_id_cleaning_operations = feature_model.extract_table_id_cleaning_operations()
    assert table_id_cleaning_operations == [
        TableIdCleaningOperation(
            table_id=snowflake_event_table_id,
            column_cleaning_operations=[
                ColumnCleaningOperation(column_name="col_float", cleaning_operations=[]),
                ColumnCleaningOperation(column_name="cust_id", cleaning_operations=[]),
                ColumnCleaningOperation(column_name="event_timestamp", cleaning_operations=[]),
            ],
        )
    ]


def test_extract_table_id_to_feature_job_settings_and_cleaning_operations__non_time_based_feature(
    non_time_based_features, snowflake_item_table_id, snowflake_event_table_id
):
    """Test extract table id to feature job settings and cleaning operations"""
    feature = non_time_based_features[0]
    feature.save()
    feature_model = feature.cached_model
    table_id_feature_job_settings = feature_model.extract_table_id_feature_job_settings()
    assert table_id_feature_job_settings == []

    table_id_cleaning_operations = feature_model.extract_table_id_cleaning_operations()
    assert table_id_cleaning_operations == [
        TableIdCleaningOperation(
            table_id=snowflake_item_table_id,
            column_cleaning_operations=[
                ColumnCleaningOperation(column_name="event_id_col", cleaning_operations=[]),
                ColumnCleaningOperation(column_name="item_amount", cleaning_operations=[]),
            ],
        ),
        TableIdCleaningOperation(table_id=snowflake_event_table_id, column_cleaning_operations=[]),
    ]


def test_feature_binary_operation_validation__feature_job_settings(
    snowflake_event_table_with_entity, feature_group_feature_job_setting, snowflake_event_table_id
):
    """Test feature binary operation validation"""
    event_view = snowflake_event_table_with_entity.get_view()
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "1w"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m", "sum_1w"],
    )
    feature_sum_30m = feature_group["sum_30m"]

    # construct a feature with different feature job setting
    assert feature_group_feature_job_setting.period == "1800s"
    feature_group_feature_job_setting.period = "3600s"
    feature_sum_1w = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["1w"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_1w"],
    )["sum_1w"]

    # construct a composite feature with different feature job setting should fail
    with pytest.raises(ValueError) as exc:
        _ = feature_sum_30m / feature_sum_1w

    expected_message = (
        f"Feature job setting (table ID: {snowflake_event_table_id}) of "
        "feature sum_30m (blind_spot='600s' period='1800s' offset='300s' execution_buffer='0s') and "
        "feature sum_1w (blind_spot='600s' period='3600s' offset='300s' execution_buffer='0s') are not consistent. "
        "Binary feature operations are only supported when the feature job settings are consistent."
    )
    assert expected_message in str(exc.value)

    # test feature info (no duplicate feature job setting, 1 feature job setting per table)
    feat_ratio = feature_sum_30m / feature_group["sum_1w"]
    feat_ratio.name = "sum_30m_over_sum_1w"
    feat_ratio.save()
    table_feature_job_setting = feat_ratio.info()["table_feature_job_setting"]
    feature_job_setting = {
        "blind_spot": "600s",
        "period": "1800s",
        "offset": "300s",
        "execution_buffer": "0s",
    }
    assert table_feature_job_setting == {
        "this": [
            {
                "table_name": "sf_event_table",
                "feature_job_setting": feature_job_setting,
            }
        ],
        "default": [
            {
                "table_name": "sf_event_table",
                "feature_job_setting": feature_job_setting,
            }
        ],
    }
