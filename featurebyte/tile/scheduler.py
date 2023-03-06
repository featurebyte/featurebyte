"""
FeatureByte Tile Scheduler
"""
from typing import Any

from datetime import datetime

from bson import ObjectId
from pydantic import BaseModel, PrivateAttr

from featurebyte.models.base import User
from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.persistent import get_persistent


class TileScheduler(BaseModel):
    """
    FeatureByte Scheduler using apscheduler
    """

    _task_manager: TaskManager = PrivateAttr()

    def __init__(self, user_id: ObjectId, workspace_id: ObjectId, **kw: Any) -> None:
        """
        Instantiate TileScheduler instance

        Parameters
        ----------
        user_id: ObjectId
            user id
        workspace_id: ObjectId
            workspace id
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._task_manager = TaskManager(
            user=User(id=user_id), persistent=get_persistent(), workspace_id=workspace_id
        )

    async def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        start_after: datetime,
        instance: Any,
        user_id: ObjectId,
        feature_store_id: ObjectId,
        workspace_id: ObjectId,
    ) -> None:
        """
        Start job with Interval seconds

        Parameters
        ----------
        job_id: str
            job id
        interval_seconds: int
            interval between runs
        start_after: datetime
            starting point for the interval calculation
        instance: Any
            instance of the class to be run
        user_id: ObjectId
            user id
        feature_store_id: ObjectId
            feature store id
        workspace_id: ObjectId
            workspace id
        """

        payload = TileTaskPayload(
            name=job_id,
            module_path=instance.__class__.__module__,
            class_name=instance.__class__.__name__,
            instance_str=instance.json(),
            user_id=user_id,
            feature_store_id=feature_store_id,
            workspace_id=workspace_id,
        )

        await self._task_manager.schedule_interval_task(
            name=job_id,
            payload=payload,
            interval=Interval(every=interval_seconds, period="seconds"),
            start_after=start_after,
        )

    async def stop_job(self, job_id: str) -> None:
        """
        Stop job

        Parameters
        ----------
        job_id: str
            job id to be stopped
        """
        await self._task_manager.delete_periodic_task_by_name(job_id)

    async def get_job_details(self, job_id: str) -> PeriodicTask:
        """
        Get Jobs from input job store

        Parameters
        ----------
        job_id: str
            job id

        Returns
        ----------
            Job Instance
        """
        return await self._task_manager.get_periodic_task_by_name(name=job_id)
