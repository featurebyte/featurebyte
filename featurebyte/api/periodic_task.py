"""
PeriodicTask class
"""
from __future__ import annotations

from typing import Optional

from featurebyte.api.api_handler.list import ListHandler
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
    _get_schema = PeriodicTaskModel

    @classmethod
    def _list_handler(cls) -> Optional[ListHandler]:
        return ListHandler(
            route=cls._route,
            list_schema=PeriodicTaskModel,
            list_fields=[
                "name",
                "created_at",
                "interval",
                "crontab",
                "total_run_count",
                "max_run_count",
                "last_run_at",
            ],
        )
