"""
Test custom scheduling logic
"""
import pandas as pd
import pytest
from celerybeatmongo.models import PeriodicTask as PeriodicTaskDoc
from freezegun import freeze_time

from featurebyte.models.periodic_task import Interval, PeriodicTask
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
        # job not triggered due to last run already happened
        ("2023-01-03 01:01:41", "2023-01-03 01:01:40", (False, 3599)),
        ("2023-01-03 01:31:40", "2023-01-03 01:01:44", (False, 1800)),
        ("2023-01-03 01:01:41", "2023-01-03 00:31:40", (True, 3599)),
    ],
)
def test_mongo_schedule_entry_due_check(current_time, last_run_at, expected):
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
        task = PeriodicTaskDoc(**periodic_task.dict())
        print(MongoScheduleEntry(task).is_due())
        assert MongoScheduleEntry(task).is_due() == expected
