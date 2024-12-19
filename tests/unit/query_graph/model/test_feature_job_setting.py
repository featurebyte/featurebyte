"""
Test module for feature job setting
"""

import pytest
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    TableFeatureJobSetting,
    TableIdFeatureJobSetting,
)


def test_equality_of_feature_job_setting():
    """
    Test equality
    """
    feature_job_setting_1 = FeatureJobSetting(blind_spot="1h", period="1d", offset="1h")
    feature_job_setting_2 = FeatureJobSetting(blind_spot="1h", period="1d", offset="1h")
    assert feature_job_setting_1 == feature_job_setting_2

    feature_job_setting_3 = FeatureJobSetting(
        blind_spot="1h", period="1d", offset="2h"
    )  # this is different
    assert feature_job_setting_1 != feature_job_setting_3

    # create a job setting equivalent to feature_job_setting_1, but with units all defined in seconds
    feature_job_setting_4 = FeatureJobSetting(
        blind_spot="3600s", period=f"{60 * 60 * 24}s", offset="3600s"
    )
    assert feature_job_setting_4 == feature_job_setting_1


def test_table_id_feature_job_setting():
    """Test table id feature job setting"""
    table_id = ObjectId()
    setting1 = TableIdFeatureJobSetting(
        table_id=table_id,
        feature_job_setting=FeatureJobSetting(blind_spot="1h", period="1d", offset="1h"),
    )
    setting2 = TableIdFeatureJobSetting(
        table_id=table_id,
        feature_job_setting=FeatureJobSetting(blind_spot="3600s", period="24h", offset="60m"),
    )
    assert setting1 == setting2

    # compare with dict
    assert setting1 == setting2.model_dump()

    # check that table setting is hashable
    assert hash(setting1) == hash(setting2)

    # change table id & compare
    setting2.table_id = ObjectId()
    assert setting1 != setting2


def test_feature_job_setting():
    """Test feature job setting with non-zero execution buffer"""
    with pytest.raises(NotImplementedError) as exc_info:
        FeatureJobSetting(blind_spot="1h", period="1d", offset="1h", execution_buffer="1h")

    expected_msg = "Setting execution_buffer is not supported."
    assert expected_msg in str(exc_info.value)


@pytest.mark.parametrize(
    "valid_crontab",
    [
        "0 0 1 * *",
        "0 0 * * *",
        "0 * * * *",
        "* * * * *",
    ],
)
def test_cron_feature_job_setting__valid_crontab_expressiom(valid_crontab):
    """Test cron feature job setting"""

    CronFeatureJobSetting(crontab=valid_crontab)


@pytest.mark.parametrize(
    "invalid_crontab",
    [
        "0.1 0 0 * *",
        "a 0 1 * *",
        "0 0 0 * *",
        "Some text",
    ],
)
def test_cron_feature_job_setting__invalid_crontab_expressiom(invalid_crontab):
    """Test cron feature job setting"""
    with pytest.raises(ValueError) as exc_info:
        CronFeatureJobSetting(crontab=invalid_crontab)

    expected_msg = f"Invalid crontab expression: {invalid_crontab}"
    assert expected_msg in str(exc_info.value)


def test_table_feature_job_setting_deserialization():
    """Test feature job setting deserialization"""
    data_1 = {
        "table_name": "table_name",
        "feature_job_setting": {
            "blind_spot": "1h",
            "period": "1d",
            "offset": "1h",
        },
    }

    data_2 = {
        "table_name": "table_name",
        "feature_job_setting": {
            "crontab": {
                "minute": 0,
                "hour": 0,
                "day_of_week": "*",
                "day_of_month": "*",
                "month_of_year": "*",
            },
            "timezone": "UTC",
        },
    }

    setting1 = TableFeatureJobSetting(**data_1)
    setting2 = TableFeatureJobSetting(**data_2)
    assert isinstance(setting1.feature_job_setting, FeatureJobSetting)
    assert isinstance(setting2.feature_job_setting, CronFeatureJobSetting)


def test_table_id_feature_job_setting_deserialization():
    """Test feature job setting deserialization"""
    table_id = ObjectId()
    data_1 = {
        "table_id": table_id,
        "feature_job_setting": {
            "blind_spot": "1h",
            "period": "1d",
            "offset": "1h",
        },
    }

    data_2 = {
        "table_id": table_id,
        "feature_job_setting": {
            "crontab": {
                "minute": 0,
                "hour": 0,
                "day_of_week": "*",
                "day_of_month": "*",
                "month_of_year": "*",
            },
            "timezone": "UTC",
        },
    }

    setting1 = TableIdFeatureJobSetting(**data_1)
    setting2 = TableIdFeatureJobSetting(**data_2)
    assert isinstance(setting1.feature_job_setting, FeatureJobSetting)
    assert isinstance(setting2.feature_job_setting, CronFeatureJobSetting)

    # check hash of table setting
    settings = {setting1, setting2}
    assert len(settings) == 2
