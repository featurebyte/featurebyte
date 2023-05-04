"""
Customized MongoDB Scheduler
"""
from typing import Any

import datetime

from celery import schedules
from celerybeatmongo.schedulers import MongoScheduleEntry as BaseMongoScheduleEntry
from celerybeatmongo.schedulers import MongoScheduler as BaseMongoScheduler


class MongoScheduleEntry(BaseMongoScheduleEntry):
    """
    Customized MongoDB Scheduler Entry
    """

    epoch_time: datetime.datetime = datetime.datetime(1970, 1, 1)

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
            # handle due check for jobs with time modulo frequency
            now = datetime.datetime.utcnow()
            last_run_at = self._task.last_run_at.replace(tzinfo=None)
            interval_seconds = datetime.timedelta(
                **{self._task.interval.period: self._task.interval.every}
            ).total_seconds()
            total_seconds = (
                now - self.epoch_time
            ).total_seconds() - self._task.time_modulo_frequency_second

            # check if we are within trigger window
            time_after_scheduled_seconds = total_seconds % interval_seconds
            next_check = int(interval_seconds - time_after_scheduled_seconds)
            if 0 <= time_after_scheduled_seconds < 5:
                # check if job already ran in this window
                if (
                    last_run_at
                    and (now - last_run_at).total_seconds() > time_after_scheduled_seconds
                ):
                    # trigger and schedule next check
                    return True, next_check
            elif (
                not last_run_at
                or (now - last_run_at).total_seconds() > time_after_scheduled_seconds
            ):
                # trigger and schedule next check
                return True, next_check
            return False, next_check

        return super().is_due()


class MongoScheduler(BaseMongoScheduler):
    """
    Customized MongoDB Scheduler
    """

    Entry = MongoScheduleEntry
