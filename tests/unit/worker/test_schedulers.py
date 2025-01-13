"""
Test custom scheduling logic
"""

import datetime

import pandas as pd
import pytest
import pytz
from celery import schedules
from celerybeatmongo.models import PeriodicTask as PeriodicTaskDoc
from freezegun import freeze_time

from featurebyte.models.periodic_task import Crontab, Interval, PeriodicTask
from featurebyte.worker.schedulers import MongoScheduleEntry


@pytest.mark.parametrize(
    "current_time,last_run_at,expected",
    [
        # job triggers within schedule window
        ("2023-01-03 01:01:40", "2023-01-03 00:01:40", (True, 3600)),
        ("2023-01-03 01:01:44", "2023-01-03 00:01:40", (True, 3596)),
        # job triggers outside window because last run is missed
        ("2023-01-03 01:01:45", "2023-01-03 00:01:40", (True, 3595)),
        ("2023-01-03 01:31:40", "2023-01-03 00:01:40", (True, 1800)),
        ("2023-01-03 01:01:40", "2023-01-03 01:01:39", (True, 3600)),
        ("2023-01-03 01:01:44", "2023-01-03 01:01:39", (True, 3596)),
        ("2023-01-03 01:31:40", "2023-01-03 00:41:40", (True, 1800)),
        # job not triggered due to last run already happened
        ("2023-01-03 01:01:41", "2023-01-03 01:01:40", (False, 3599)),
        ("2023-01-03 01:31:40", "2023-01-03 01:01:44", (False, 1800)),
        ("2023-01-03 01:01:41", "2023-01-03 00:31:40", (True, 3599)),
    ],
)
def test_mongo_interval_schedule_entry_due_check(current_time, last_run_at, expected):
    """
    Test MongoScheduleEntry logic for checking if a task is due to run
    """
    with freeze_time(current_time):
        periodic_task = PeriodicTask(
            name="task",
            task="featurebyte.worker.task_executor.execute_io_task",
            interval=Interval(every=1, period="hours"),
            args=[],
            kwargs={},
            time_modulo_frequency_second=100,
            last_run_at=pd.to_datetime(last_run_at),
            queue="io_task",
        )
        task = PeriodicTaskDoc(**periodic_task.model_dump())
        assert MongoScheduleEntry(task).is_due() == expected


@pytest.mark.parametrize("timezone", [None, "America/New_York"])
@pytest.mark.parametrize(
    "schedule_params",
    [
        {
            "crontab": Crontab(
                minute="0",
                hour="1,2",
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
        },
        {
            "interval": Interval(every=1, period="hours"),
        },
    ],
)
def test_mongo_schedule_entry_with_timezone(schedule_params, timezone):
    """
    Test MongoScheduleEntry logic nowfun is set correctly
    """
    periodic_task = PeriodicTask(
        name="task",
        task="featurebyte.worker.task_executor.execute_io_task",
        args=[],
        kwargs={},
        last_run_at=pd.to_datetime("2023-01-03 01:00:00"),
        queue="io_task",
        timezone=timezone,
        **schedule_params,
    )
    task = PeriodicTaskDoc(**periodic_task.model_dump())
    schedule_entry = MongoScheduleEntry(task)
    schedule = schedule_entry.schedule

    # check schedule is set correctly
    if "interval" in schedule_params:
        assert isinstance(schedule, schedules.schedule)
        assert schedule.run_every.total_seconds() == 3600

    if "crontab" in schedule_params:
        assert isinstance(schedule, schedules.crontab)
        assert schedule.minute == {0}
        assert schedule.hour == {1, 2}
        assert schedule.day_of_week == set(range(7))
        assert schedule.day_of_month == set(range(1, 32))
        assert schedule.month_of_year == set(range(1, 13))

    # check default now function is set correctly
    now_value = schedule_entry.default_now()
    expected_now_val = datetime.datetime.now(pytz.timezone(timezone or "UTC"))
    assert expected_now_val - now_value < datetime.timedelta(seconds=1)
