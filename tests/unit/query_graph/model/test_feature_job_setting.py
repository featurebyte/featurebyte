"""
Test module for feature job setting
"""

import pytest
from bson import ObjectId

from featurebyte import CalendarWindow, Crontab, FeatureJobSetting
from featurebyte.exception import CronFeatureJobSettingConversionError
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


def test_cron_feature_job_setting__invalid_blind_spot():
    """Test cron feature job setting with invalid crontab expression"""
    with pytest.raises(ValueError) as exc_info:
        CronFeatureJobSetting(crontab="0 * * * *", blind_spot="NaNs")

    expected_msg = "Invalid blind spot: NaNs (unit abbreviation w/o a number)"
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


@pytest.mark.parametrize(
    "crontab_expr, blind_spot, expected",
    [
        # Every hour at 10 minutes past
        ("10 * * * *", "200s", FeatureJobSetting(period="3600s", offset="600s", blind_spot="200s")),
        # Every 2 hour
        ("0 */2 * * *", "300s", FeatureJobSetting(period="7200s", offset="0s", blind_spot="300s")),
        # Daily at midnight
        ("0 0 * * *", "600s", FeatureJobSetting(period="86400s", offset="0s", blind_spot="600s")),
        # Weekly on Monday at 2 AM
        (
            "0 2 * * 1",
            "1000s",
            FeatureJobSetting(period="604800s", offset="352800s", blind_spot="1000s"),
        ),
        # CalendarWindow blind spot
        (
            "10 * * * *",
            CalendarWindow(unit="MINUTE", size=5),
            FeatureJobSetting(period="3600s", offset="600s", blind_spot="300s"),
        ),
    ],
)
def test_to_feature_job_setting(crontab_expr, blind_spot, expected):
    """Test to_feature_job_setting with valid cron schedules and required blind_spot"""
    cron_job = CronFeatureJobSetting(
        crontab=crontab_expr, blind_spot=blind_spot, timezone="Etc/UTC"
    )
    result = cron_job.to_feature_job_setting()
    assert result == expected


@pytest.mark.parametrize(
    "crontab_expr",
    [
        "0 10,11 * * *",  # Runs at hour 10 and 11, not a fixed interval
        "0 2 * * 1,3",  # Runs on Monday and Wednesday, not periodic
        "0 0 1 1,6 *",  # Runs on Jan 1st and June 1st, gap is inconsistent
        "0 3 1 * *",  # Runs on the 1st of each month, months have different lengths
    ],
)
def test_to_feature_job_setting_failure(crontab_expr):
    """Test that non-periodic cron schedules fail conversion"""
    cron_job = CronFeatureJobSetting(crontab=crontab_expr, blind_spot="200s", timezone="Etc/UTC")

    with pytest.raises(
        CronFeatureJobSettingConversionError,
        match="cron schedule does not result in a fixed interval",
    ):
        cron_job.to_feature_job_setting()


@pytest.mark.parametrize(
    "invalid_timezone",
    ["America/New_York", "Asia/Tokyo", "Europe/London"],
)
def test_to_feature_job_setting_invalid_timezone(invalid_timezone):
    """Test that a non-UTC timezone raises an error"""
    cron_job = CronFeatureJobSetting(
        crontab="10 * * * *", blind_spot="200s", timezone=invalid_timezone
    )

    with pytest.raises(CronFeatureJobSettingConversionError, match="timezone must be UTC"):
        cron_job.to_feature_job_setting()


def test_to_feature_job_setting_missing_blind_spot():
    """Test that missing blind_spot raises an error when creating CronFeatureJobSetting"""
    cron_job = CronFeatureJobSetting(crontab="10 * * * *", blind_spot=None, timezone="Etc/UTC")
    with pytest.raises(
        CronFeatureJobSettingConversionError,
        match="blind_spot is not specified",
    ):
        cron_job.to_feature_job_setting()


def test_to_feature_job_setting_non_fixed_size_blind_spot():
    """Test that missing blind_spot raises an error when creating CronFeatureJobSetting"""
    cron_job = CronFeatureJobSetting(
        crontab="10 * * * *", blind_spot=CalendarWindow(unit="MONTH", size=1), timezone="Etc/UTC"
    )
    with pytest.raises(
        CronFeatureJobSettingConversionError,
        match="blind_spot is not a fixed size window",
    ):
        cron_job.to_feature_job_setting()


@pytest.mark.parametrize(
    "setting_1,setting_2,expected",
    [
        (
            CronFeatureJobSetting(crontab="10 * * * *"),
            CronFeatureJobSetting(crontab="10 * * * *"),
            True,
        ),
        (
            CronFeatureJobSetting(
                crontab=Crontab(
                    minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"
                )
            ),
            CronFeatureJobSetting(
                crontab=Crontab(
                    minute="0", hour="0", day_of_month="*", month_of_year="*", day_of_week="*"
                )
            ),
            True,
        ),
        (
            CronFeatureJobSetting(crontab="10 * * * *", blind_spot="200s"),
            FeatureJobSetting(period="3600s", offset="600s", blind_spot="200s"),
            True,
        ),
        (
            CronFeatureJobSetting(crontab="10 * * * 1", blind_spot="200s"),
            FeatureJobSetting(period="3600s", offset="600s", blind_spot="200s"),
            False,
        ),
    ],
)
def test_feature_job_setting_comparison(setting_1, setting_2, expected):
    """
    Test comparison of feature job settings
    """
    if expected:
        assert setting_1 == setting_2
        assert setting_2 == setting_1
    else:
        assert setting_1 != setting_2
        assert setting_2 != setting_1


@pytest.mark.parametrize(
    "setting,expected",
    [
        (
            CronFeatureJobSetting(crontab="10 * * * *", blind_spot="300s"),
            CalendarWindow(unit="MINUTE", size=5),
        ),
        (
            CronFeatureJobSetting(
                crontab="10 * * * *", blind_spot=CalendarWindow(unit="DAY", size=3)
            ),
            CalendarWindow(unit="DAY", size=3),
        ),
    ],
)
def test_get_calendar_window_blind_spot(setting, expected):
    """
    Test get_calendar_window_blind_spot
    """
    actual = setting.get_blind_spot_calendar_window()
    assert actual == expected
