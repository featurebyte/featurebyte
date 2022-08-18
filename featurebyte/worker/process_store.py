"""
This module contains ProcessStore class
"""
from __future__ import annotations

from typing import Any, Callable, Optional, Type

from enum import Enum
from multiprocessing import Process

from bson.objectid import ObjectId
from cachetools import FIFOCache

from featurebyte.common.singleton import SingletonMeta
from featurebyte.worker.enum import Command
from featurebyte.worker.task.base import TASK_PAYLOAD_MAP
from featurebyte.worker.task_executor import TaskExecutor


class ProcessStore(metaclass=SingletonMeta):
    """
    ProcessStore class is responsible to store process temporary
    """

    _store: FIFOCache[tuple[Optional[ObjectId], ObjectId], Process] = FIFOCache(maxsize=128)
    _command_class: Type[Enum] = Command
    _task_executor: Callable[..., Any] = TaskExecutor

    def submit(self, payload: dict[str, Any]) -> ObjectId:
        """
        Submit payload to initiate a new process

        Parameters
        ----------
        payload: dict[str, Any]
            Payload used to initiate the process

        Returns
        -------
        ObjectId
        """
        task_status_id = ObjectId()
        payload_obj = TASK_PAYLOAD_MAP[self._command_class(payload["command"])](**payload)
        process = Process(target=self._task_executor, args=(payload_obj.dict(),), daemon=True)
        process.start()
        self._store[(payload_obj.user_id, task_status_id)] = process
        return task_status_id

    def get(self, user_id: Optional[ObjectId], task_status_id: ObjectId) -> Optional[Process]:
        """
        Retrieve process given user_id and task_status_id

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID
        task_status_id: ObjectId
            Task status ID

        Returns
        -------
        Optional[Process]
        """
        key_pair = (user_id, task_status_id)
        return self._store.get(key_pair)

    def list(self, user_id: Optional[ObjectId]) -> list[tuple[ObjectId, Process]]:
        """
        List process of the given user

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID

        Returns
        -------
        List of (task_status_id, process) tuples
        """
        output = []
        for (user_id_key, task_status_id_key), process in self._store.items():
            if user_id_key == user_id:
                output.append((task_status_id_key, process))
        return output
