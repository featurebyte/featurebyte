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
            feature_job_setting=FeatureJobSetting(
                blind_spot="600s",
                frequency="1800s",
                time_modulo_frequency="300s",
            ),
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
    feature_sum_30m = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )["sum_30m"]

    # construct a feature with different feature job setting
    assert feature_group_feature_job_setting.frequency == "30m"
    feature_group_feature_job_setting.frequency = "1h"
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
        "feature sum_30m (blind_spot='600s' frequency='1800s' time_modulo_frequency='300s') and "
        "feature sum_1w (blind_spot='600s' frequency='3600s' time_modulo_frequency='300s') are not consistent. "
        "Binary feature operations are only supported when the feature job settings are consistent."
    )
    assert expected_message in str(exc.value)
