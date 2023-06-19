"""
PeriodicTask class
"""
from __future__ import annotations

from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.periodic_task import PeriodicTask as PeriodicTaskModel


class PeriodicTask(PeriodicTaskModel, SavableApiObject):
    """
    PeriodicTask class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc()

    # class variables
    _route = "/periodic_task"
    _list_schema = PeriodicTaskModel
    _get_schema = PeriodicTaskModel
    _list_fields = [
        "name",
        "created_at",
        "interval",
        "crontab",
        "total_run_count",
        "max_run_count",
        "last_run_at",
    ]
