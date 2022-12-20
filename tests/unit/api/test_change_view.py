"""
Unit tests for change view

Note that we don't currently inherit from the base view test suite as there are quite a few differences. I'll
work on updating that in a follow-up.
"""
import pytest

from featurebyte import FeatureJobSetting
from featurebyte.api.change_view import ChangeView


@pytest.fixture(name="change_view_default_feature_job_setting")
def get_change_view_default_feature_job_setting():
    """
    Fixture to get the default feature job setting
    """
    return FeatureJobSetting(
        blind_spot="0",
        time_modulo_frequency="0",
        frequency="24h",
    )


def test_get_default_feature_job_setting(change_view_default_feature_job_setting):
    """
    Test get_default_feature_job_setting
    """
    # default is returned if nothing is provided
    feature_job_setting = ChangeView.get_default_feature_job_setting()
    assert feature_job_setting == change_view_default_feature_job_setting

    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    # get back setting provided
    feature_job_setting = ChangeView.get_default_feature_job_setting(job_setting_provided)
    assert feature_job_setting == job_setting_provided


def test_validate_inputs(snowflake_scd_data):
    """
    Test _validate_inputs
    """
    # empty input should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_inputs(snowflake_scd_data, "")
    assert "Empty column provided" in str(exc_info)

    # column not in SCD data should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_inputs(snowflake_scd_data, "random_col")
    assert "Column provided is not a column in the SlowlyChangingData provided" in str(exc_info)

    # column in SCD data should be ok
    ChangeView._validate_inputs(snowflake_scd_data, "col_int")


def test_get_new_column_names():
    """
    Test _get_new_column_names
    """
    col_name = "col_name"
    old_col, new_col = ChangeView._get_new_column_names(col_name)
    assert old_col == f"past_{col_name}"
    assert new_col == f"new_{col_name}"


def change_view_test_helper(snowflake_scd_data, change_view):
    """
    Helper method to do some asserts
    """
    assert len(change_view.columns_info) == 4
    assert change_view.timestamp_column == snowflake_scd_data.effective_timestamp_column
    assert change_view.natural_key_column == snowflake_scd_data.natural_key_column
    assert change_view.columns == ["col_text", "event_timestamp", "new_col_int", "past_col_int"]


def test_from_scd_data__no_default_job_setting(
    snowflake_scd_data, change_view_default_feature_job_setting
):
    """
    Test from_scd_data - no default job setting provided
    """
    change_view = ChangeView.from_scd_data(snowflake_scd_data, "col_int")
    assert change_view.default_feature_job_setting == change_view_default_feature_job_setting
    change_view_test_helper(snowflake_scd_data, change_view)


def test_from_scd_data__with_default_job_setting(snowflake_scd_data):
    """
    Test from_scd_data - default job setting provided
    """
    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    change_view = ChangeView.from_scd_data(snowflake_scd_data, "col_int", job_setting_provided)
    assert change_view.default_feature_job_setting == job_setting_provided
    change_view_test_helper(snowflake_scd_data, change_view)
