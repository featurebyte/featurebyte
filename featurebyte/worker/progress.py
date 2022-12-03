"""
This module contains Progress class which is used for tracking progress update
"""
from __future__ import annotations

from typing import Any, Optional

from abc import abstractmethod
from multiprocessing import Queue

from bson.objectid import ObjectId

from featurebyte.common.singleton import SingletonMeta


class AbstractProgress:
    """
    AbstractProgress defines interface for Progress
    """

    @abstractmethod
    def get_progress(self, user_id: ObjectId, task_id: ObjectId) -> Any:
        """
        Get progress queue

        Parameters
        ----------
        user_id: ObjectId
            User ID
        task_id: ObjectId
            Task ID

        Returns
        -------
        Any
        """


class GlobalProgress(AbstractProgress, metaclass=SingletonMeta):
    """
    ProgressSink class
    """

    _queue_map: dict[tuple[Optional[ObjectId], ObjectId], Any] = {}

    def get_progress(self, user_id: Optional[ObjectId], task_id: ObjectId) -> Any:
        key_pair = (user_id, task_id)
        if key_pair not in self._queue_map:
            self._queue_map[key_pair] = Queue()
        return self._queue_map[key_pair]
