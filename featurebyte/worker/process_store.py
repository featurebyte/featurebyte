"""
This module contains ProcessStore class
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Type

import json
from multiprocessing import Process, Queue

from bson.objectid import ObjectId
from cachetools import FIFOCache

from featurebyte.common.singleton import SingletonMeta
from featurebyte.enum import StrEnum
from featurebyte.schema.task import TaskStatus
from featurebyte.worker.enum import Command
from featurebyte.worker.progress import GlobalProgress
from featurebyte.worker.task_executor import TaskExecutor


class ProcessStore(metaclass=SingletonMeta):
    """
    ProcessStore class is responsible to store process temporary
    """

    _store: FIFOCache[tuple[Optional[ObjectId], ObjectId], Dict[str, Any]] = FIFOCache(maxsize=128)
    _command_class: Type[StrEnum] = Command
    _task_executor: Callable[..., Any] = TaskExecutor

    async def submit(self, payload: str, output_path: Optional[str]) -> ObjectId:
        """
        Submit payload to initiate a new process

        Parameters
        ----------
        payload: str
            JSON payload used to initiate the process
        output_path: str
            Output path used to retrieve the result

        Returns
        -------
        ObjectId
        """
        task_id = ObjectId()
        payload_dict = json.loads(payload)
        user_id = None
        if payload_dict["user_id"]:
            user_id = ObjectId(payload_dict["user_id"])
        progress_queue = GlobalProgress().get_progress(user_id=user_id, task_id=task_id)
        queue: Any = Queue()
        process = Process(
            target=self._task_executor, args=(payload_dict, queue, progress_queue), daemon=True
        )
        process.start()
        self._store[(user_id, task_id)] = {
            "process": process,
            "payload": payload_dict,
            "output_path": output_path,
            "queue": queue,
        }
        return task_id

    async def get(self, user_id: Optional[ObjectId], task_id: ObjectId) -> Optional[dict[str, Any]]:
        """
        Retrieve process given user_id and task_id

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID
        task_id: ObjectId
            Task ID

        Returns
        -------
        Optional[dict[str, Any]]
        """
        key_pair = (user_id, task_id)
        process_data = self._store.get(key_pair)
        if process_data:
            process = process_data["process"]
            traceback = None
            if process.exitcode is None:
                status = TaskStatus.STARTED
            elif process.exitcode == 0:
                output = process_data["queue"].get()
                output_dict = json.loads(output)
                process_data["queue"].put(output)
                if "output" in output_dict:
                    status = TaskStatus.SUCCESS
                else:
                    status = TaskStatus.FAILURE
                    traceback = output_dict["traceback"]
            else:
                status = TaskStatus.FAILURE
            return {**process_data, "id": task_id, "status": status, "traceback": traceback}
        return process_data

    async def list(
        self, user_id: Optional[ObjectId]
    ) -> list[tuple[ObjectId, Optional[Dict[str, Any]]]]:
        """
        List process of the given user

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID

        Returns
        -------
        List of (task_id, process_data_dict) tuples
        """
        output = []
        for user_id_key, task_id_key in self._store.keys():
            if user_id_key == user_id:
                process_data_dict = await self.get(user_id=user_id_key, task_id=task_id_key)
                output.append((task_id_key, process_data_dict))
        return output
