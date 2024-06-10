"""
PeriodicTask class
"""

from __future__ import annotations

from typing import Any, ClassVar, List

from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.periodic_task import PeriodicTask as PeriodicTaskModel


class PeriodicTask(PeriodicTaskModel, SavableApiObject):
    """
    PeriodicTask class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()
    _route: ClassVar[str] = "/periodic_task"
    _list_schema: ClassVar[Any] = PeriodicTaskModel
    _get_schema: ClassVar[Any] = PeriodicTaskModel
    _list_fields: ClassVar[List[str]] = [
        "name",
        "created_at",
        "interval",
        "crontab",
        "total_run_count",
        "max_run_count",
        "last_run_at",
    ]
