"""
Unit tests for change view

Note that we don't currently inherit from the base view test suite as there are quite a few differences. I'll
work on updating that in a follow-up.
"""
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from featurebyte import FeatureJobSetting
from featurebyte.api.change_view import ChangeView


def test_get_default_feature_job_setting():
    """
    Test get_default_feature_job_setting
    """
    # default is returned if nothing is provided
    datetime_mock = Mock(wraps=datetime)
    mocked_hour = 11
    mocked_minute = 15
    datetime_mock.now.return_value = datetime(1999, 1, 1, mocked_hour, mocked_minute, 0)
    with patch("featurebyte.api.change_view.datetime", new=datetime_mock):
        feature_job_setting = ChangeView.get_default_feature_job_setting()
        assert feature_job_setting == FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{mocked_hour}h{mocked_minute}m",
            frequency="24h",
        )

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


def test_validate_prefixes():
    """
    Test _validate_prefixes
    """
    # No error expected
    ChangeView._validate_prefixes(None)

    # Both None should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes((None, None))
    assert "Prefixes provided are both None" in str(exc_info)

    # Empty string in second position should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("old", ""))
    assert "Please provide a non-empty string as a prefix value" in str(exc_info)

    # Empty string in first position should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("", "new"))
    assert "Please provide a non-empty string as a prefix value" in str(exc_info)

    # Same prefix should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("same_prefix", "same_prefix"))
    assert "Prefixes provided need to be different values" in str(exc_info)


def test_get_new_column_names():
    """
    Test _get_new_column_names
    """
    col_name = "col_name"
    old_col, new_col = ChangeView._get_new_column_names(col_name, None)
    assert old_col == f"past_{col_name}"
    assert new_col == f"new_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, (None, "updated_"))
    assert old_col == f"past_{col_name}"
    assert new_col == f"updated_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, ("prior_", None))
    assert old_col == f"prior_{col_name}"
    assert new_col == f"new_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, ("prior_", "updated_"))
    assert old_col == f"prior_{col_name}"
    assert new_col == f"updated_{col_name}"


def change_view_test_helper(snowflake_scd_data, change_view):
    """
    Helper method to do some asserts
    """
    assert len(change_view.columns_info) == 4
    assert change_view.timestamp_column == snowflake_scd_data.effective_timestamp_column
    assert change_view.natural_key_column == snowflake_scd_data.natural_key_column
    assert change_view.columns == ["col_text", "event_timestamp", "new_col_int", "past_col_int"]


def test_from_scd_data__no_default_job_setting(snowflake_scd_data):
    """
    Test from_slowly_changing_data - no default job setting provided
    """
    datetime_mock = Mock(wraps=datetime)
    mocked_hour = 11
    mocked_minute = 15
    datetime_mock.now.return_value = datetime(1999, 1, 1, mocked_hour, mocked_minute, 0)
    with patch("featurebyte.api.change_view.datetime", new=datetime_mock):
        change_view = ChangeView.from_slowly_changing_data(snowflake_scd_data, "col_int")
        assert change_view.default_feature_job_setting == FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{mocked_hour}h{mocked_minute}m",
            frequency="24h",
        )
        change_view_test_helper(snowflake_scd_data, change_view)


def test_from_slowly_changing_data__with_default_job_setting(snowflake_scd_data):
    """
    Test from_slowly_changing_data - default job setting provided
    """
    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data, "col_int", job_setting_provided
    )
    assert change_view.default_feature_job_setting == job_setting_provided
    change_view_test_helper(snowflake_scd_data, change_view)


def test_update_feature_job_setting(snowflake_change_view):
    """
    Test update feature job setting
    """
    # Assert that a feature job setting exists
    assert snowflake_change_view.default_feature_job_setting is not None

    new_feature_job_setting = FeatureJobSetting(
        blind_spot="15m",
        time_modulo_frequency="30m",
        frequency="1h",
    )
    snowflake_change_view.update_default_feature_job_setting(new_feature_job_setting)
    assert snowflake_change_view.default_feature_job_setting == new_feature_job_setting
