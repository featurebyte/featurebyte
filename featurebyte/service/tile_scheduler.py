"""
TileSchedulerService class
"""
from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.base_service import BaseService
from featurebyte.service.task_manager import TaskManager


class TileSchedulerService(BaseService):
    """
    TileSchedulerService is responsible for scheduling tile tasks
    """

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        task_manager: TaskManager,
    ):
        super().__init__(user, persistent, catalog_id)
        self.task_manager = task_manager

    async def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        time_modulo_frequency_second: int,
        instance: Any,
        user_id: Optional[ObjectId],
        feature_store_id: ObjectId,
        catalog_id: ObjectId,
    ) -> None:
        """
        Start job with Interval seconds

        Parameters
        ----------
        job_id: str
            job id
        interval_seconds: int
            interval between runs
        time_modulo_frequency_second: int
            time modulo frequency in seconds
        instance: Any
            instance of the class to be run
        user_id: Optional[ObjectId]
            input user id
        feature_store_id: ObjectId
            feature store id
        catalog_id: ObjectId
            catalog id
        """

        payload = TileTaskPayload(
            name=job_id,
            module_path=instance.__class__.__module__,
            class_name=instance.__class__.__name__,
            instance_str=instance.json(),
            user_id=user_id if user_id else self.task_manager.user.id,
            feature_store_id=feature_store_id,
            catalog_id=catalog_id,
        )

        await self.task_manager.schedule_interval_task(
            name=job_id,
            payload=payload,
            interval=Interval(every=interval_seconds, period="seconds"),
            time_modulo_frequency_second=time_modulo_frequency_second,
        )

    async def stop_job(self, job_id: str) -> None:
        """
        Stop job

        Parameters
        ----------
        job_id: str
            job id to be stopped
        """
        await self.task_manager.delete_periodic_task_by_name(job_id)

    async def get_job_details(self, job_id: str) -> Optional[PeriodicTask]:
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
        return await self.task_manager.get_periodic_task_by_name(name=job_id)
