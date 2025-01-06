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
    ],
)
def test_cron_feature_job_setting__valid_crontab_expression(valid_crontab):
    """Test cron feature job setting with valid crontab expression"""

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
def test_cron_feature_job_setting__invalid_crontab_expression(invalid_crontab):
    """Test cron feature job setting with invalid crontab expression"""
    with pytest.raises(ValueError) as exc_info:
        CronFeatureJobSetting(crontab=invalid_crontab)

    expected_msg = f"Invalid crontab expression: {invalid_crontab}"
    assert expected_msg in str(exc_info.value)


@pytest.mark.parametrize(
    "invalid_crontab",
    [
        "* * * * *",
        "*/15 * * * *",
    ],
)
def test_cron_feature_job_setting__too_frequent_crontab(invalid_crontab):
    """Test cron feature job setting with too frequent crontab expression"""
    with pytest.raises(ValueError) as exc_info:
        CronFeatureJobSetting(crontab=invalid_crontab)

    expected_msg = "Cron schedule more frequent than hourly is not supported."
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


@pytest.mark.parametrize(
    "crontab_expr,expected",
    [
        ("0 * * * *", "hourly"),  # Every hour
        ("0 */3 * * *", "hourly"),  # Every 3 hours
        ("0 0 * * *", "daily"),  # Every day at midnight
        ("15 14 * * *", "daily"),  # Every day at a specific time
        ("0 0 */2 * *", "daily"),  # Every two days at midnight
        ("0 0 * * MON-FRI", "daily"),  # Every weekday at midnight
        ("0 9 * * 1-5", "daily"),  # Every weekday at 9:00 AM
        ("30 3 * * 0", "weekly"),  # Every Sunday at 3:30 AM
        ("0 0 * * 6,0", "weekly"),  # Every Saturday and Sunday at midnight
        ("0 0 * * 1,3,5", "weekly"),  # Every Monday, Wednesday, and Friday at midnight
        ("0 0 * * 1", "weekly"),  # Every Monday at midnight
        ("0 22 * * 1-5", "daily"),  # Every specific weekdays (Monday through Friday) at 22:00
        # ("0 17 * * 5L", "monthly"),  # Every last Friday of the month at 17:00 (Non-standard)
        ("0 0 * * 1#1", "monthly"),  # Every first Monday of the month at midnight (Non-standard)
        ("59 23 L * *", "monthly"),  # Last day of every month at 23:59 (Non-standard)
        ("0 0 5 * *", "monthly"),  # Every 5th day of the month at midnight
        ("0 0 1 * *", "monthly"),  # Every 1st of the month at midnight
        ("0 12 1,15 * *", "monthly"),  # Every 1st and 15th of the month at noon
        ("0 0 1 1,4,7,10 *", "monthly"),  # Every 1st of January, April, July, October at midnight
        ("0 0 1 1 *", "yearly"),  # Every 1st of January at midnight
        ("0 0 29 2 *", "yearly"),  # Every leap day at midnight
        ("0 0 1 1 *", "yearly"),  # Every year on January 1st at midnight
        ("0 0 1 7 *", "yearly"),  # Every year on July 1st at midnight
    ],
)
def test_extract_offline_store_feature_table_name_postfix(crontab_expr, expected):
    """Test extracting offline store feature table name postfix"""
    fjs = CronFeatureJobSetting(crontab=crontab_expr)
    assert fjs.extract_offline_store_feature_table_name_postfix(10) == expected
