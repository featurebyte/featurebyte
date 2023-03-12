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

    def is_due(self) -> Any:
        if not self._task.enabled:
            return schedules.schedstate(False, 9999)  # move behind other tasks.
        if hasattr(self._task, "start_after") and self._task.start_after:
            if datetime.datetime.now() < self._task.start_after:
                return schedules.schedstate(
                    False, (self._task.start_after - datetime.datetime.now()).total_seconds()
                )
        return super().is_due()


class MongoScheduler(BaseMongoScheduler):
    """
    Customized MongoDB Scheduler
    """

    Entry = MongoScheduleEntry
