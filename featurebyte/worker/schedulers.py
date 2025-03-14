"""
Customized MongoDB Scheduler
"""

import datetime
from typing import Any

import pytz
from celery import schedules
from celerybeatmongo.schedulers import MongoScheduleEntry as BaseMongoScheduleEntry
from celerybeatmongo.schedulers import MongoScheduler as BaseMongoScheduler


class MongoScheduleEntry(BaseMongoScheduleEntry):
    """
    Customized MongoDB Scheduler Entry
    """

    epoch_time: datetime.datetime = datetime.datetime(1970, 1, 1)

    def _default_now(self) -> Any:
        if hasattr(self._task, "timezone"):
            timezone = getattr(self._task, "timezone")
            if timezone:
                return datetime.datetime.now(pytz.timezone(timezone))
        return super()._default_now()

    def default_now(self) -> Any:
        return self._default_now()

    def next(self) -> Any:
        self._task.last_run_at = self._default_now()
        self._task.total_run_count += 1
        self._task.run_immediately = False
        return self.__class__(self._task)

    __next__ = next

    def is_due(self) -> Any:
        """
        Check if the job is due to run

        Returns
        -------
        Any
        """

        if not self._task.enabled:
            return schedules.schedstate(False, 9999)  # move behind other tasks.

        if (
            hasattr(self._task, "time_modulo_frequency_second")
            and self._task.time_modulo_frequency_second is not None
        ):
            # handle due check for jobs with time modulo frequency:
            # - attempt to launch a job as early as possible within the interval
            # - if the last run was in the current interval, we are done

            # get current time and last run time in utc
            now = datetime.datetime.utcnow()
            last_run_at = self._task.last_run_at.replace(tzinfo=None)

            # compute total seconds since epoch shifted by time modulo frequency
            interval_seconds = datetime.timedelta(**{
                self._task.interval.period: self._task.interval.every
            }).total_seconds()
            total_seconds = (
                now - self.epoch_time
            ).total_seconds() - self._task.time_modulo_frequency_second

            # how many seconds has elapsed since the start of the current interval
            seconds_since_interval_start = total_seconds % interval_seconds

            # how many seconds to next interval
            seconds_to_next_interval = int(interval_seconds - seconds_since_interval_start)

            # if the last run did not happen in the current interval, we are due
            if (now - last_run_at).total_seconds() > seconds_since_interval_start:
                # trigger and and check when next interval starts
                return True, seconds_to_next_interval

            # last run happened in the current interval, wait until next interval
            return False, seconds_to_next_interval

        return super().is_due()


class MongoScheduler(BaseMongoScheduler):
    """
    Customized MongoDB Scheduler
    """

    Entry = MongoScheduleEntry
