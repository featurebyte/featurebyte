"""
This module contains Progress class which is used for tracking progress update
"""
from __future__ import annotations

from typing import Any

from multiprocessing import Queue

from bson.objectid import ObjectId

from featurebyte.common.singleton import SingletonMeta


class GlobalProgress(metaclass=SingletonMeta):
    """
    ProgressSink class
    """

    # pylint: disable=too-few-public-methods

    _queue_map: dict[tuple[ObjectId, ObjectId], Any] = {}

    @classmethod
    def get_progress(cls, user_id: ObjectId, task_status_id: ObjectId) -> Any:
        """
        Get progress queue

        Parameters
        ----------
        user_id: ObjectId
            User ID
        task_status_id: ObjectId
            Task Status ID

        Returns
        -------
        Queue
        """
        key_pair = (user_id, task_status_id)
        if key_pair not in cls._queue_map:
            cls._queue_map[key_pair] = Queue()
        return cls._queue_map[key_pair]
