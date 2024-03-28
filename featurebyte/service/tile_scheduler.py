"""
TileSchedulerService class
"""

from typing import Optional

from bson import ObjectId

from featurebyte.models.base import User
from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.models.tile import TileScheduledJobParameters
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.task_manager import TaskManager


class TileSchedulerService:
    """
    TileSchedulerService is responsible for scheduling tile tasks
    """

    def __init__(
        self,
        user: User,
        catalog_id: ObjectId,
        task_manager: TaskManager,
    ):
        self.user = user
        self.catalog_id = catalog_id
        self.task_manager = task_manager

    async def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        time_modulo_frequency_second: int,
        parameters: TileScheduledJobParameters,
        feature_store_id: ObjectId,
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
        parameters: TileScheduledJobParameters
            Tile scheduled job parameters
        feature_store_id: ObjectId
            feature store id
        """
        payload = TileTaskPayload(
            name=job_id,
            user_id=self.user.id,
            feature_store_id=feature_store_id,
            catalog_id=self.catalog_id,
            parameters=parameters,
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
