"""
FeatureByte Tile Scheduler
"""
from typing import Any, Dict, List

from datetime import datetime

from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from celery.schedules import schedule
from pydantic import BaseModel, PrivateAttr
from redbeat import RedBeatSchedulerEntry

from featurebyte.logger import logger
from featurebyte.tile.celery.tasks import celery_app


class TileScheduler(BaseModel):
    """
    FeatureByte Scheduler using apscheduler
    """

    _scheduler: BackgroundScheduler = PrivateAttr()
    _job_store: str = PrivateAttr()
    _tile_scheduler_map: Dict[str, Any] = PrivateAttr()

    def __init__(self, job_store: str = "default", **kw: Any) -> None:
        """
        Instantiate TileScheduler instance

        Parameters
        ----------
        job_store: str
            target job store name
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._job_store = job_store
        self._scheduler = BackgroundScheduler()
        self._scheduler.start()

    def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        start_from: datetime,
        instance: Any,
    ) -> None:
        """
        Start job with Interval seconds

        Parameters
        ----------
        job_id: str
            job id
        interval_seconds: int
            interval between runs
        start_from: datetime
            starting point for the interval calculation
        instance: Any
            input instance
        """

        onetime_trigger = CronTrigger(
            second=start_from.second,
            minute=start_from.minute,
            hour=start_from.hour,
            day=start_from.day,
            month=start_from.month,
            year=start_from.year,
            timezone="utc",
        )

        module_path = instance.__class__.__module__
        class_name = instance.__class__.__name__
        instance_str = instance.json()
        entry = RedBeatSchedulerEntry(
            name=job_id,
            task="featurebyte.tile.celery.tasks.execute",
            schedule=schedule(interval_seconds),
            args=[module_path, class_name, instance_str],
            app=celery_app,
        )

        self._scheduler.add_job(
            id=job_id,
            jobstore=self._job_store,
            func=entry.save,
            trigger=onetime_trigger,
        )

    def stop_job(self, job_id: str) -> None:
        """
        Stop job

        Parameters
        ----------
        job_id: str
            job id to be stopped
        """
        try:
            self._scheduler.remove_job(job_id=job_id, jobstore=self._job_store)
            entry = RedBeatSchedulerEntry(name=job_id, app=celery_app)
            entry.delete()
            logger.info(f"{job_id} removed successfully")
        except JobLookupError:
            logger.warning(f"{job_id} does not exist")

    def get_job_details(self, job_id: str) -> Job:
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
        return self._scheduler.get_job(job_id=job_id)

    def get_jobs(self) -> List[str]:
        """
        Get Jobs from input job store
        Returns
        ----------
            List of job ids
        """
        jobs = self._scheduler.get_jobs()
        return [job.id for job in jobs]


class TileSchedulerFactory:
    """
    Factory for TileScheduler
    """

    tile_scheduler_instance = None

    @classmethod
    def get_instance(cls) -> TileScheduler:
        """
        Retrieve the TileScheduler instance

        Returns
        -------
            TileScheduler instance
        """
        if not cls.tile_scheduler_instance:
            cls.tile_scheduler_instance = TileScheduler()

        return cls.tile_scheduler_instance
