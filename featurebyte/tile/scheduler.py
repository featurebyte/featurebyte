"""
FeatureByte Tile Scheduler
"""
from typing import Any, Dict, List, Optional

from datetime import datetime

from apscheduler.job import Job
from apscheduler.jobstores.base import JobLookupError
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, PrivateAttr

from featurebyte.logger import logger

JOB_STORES: Dict[str, Any] = {
    "default": MemoryJobStore(),
    "local": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite"),
    # "mongo": MongoDBJobStore(),
}


class TileScheduler(BaseModel):
    """
    FeatureByte Scheduler using apscheduler
    """

    _scheduler: AsyncIOScheduler = PrivateAttr()
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
        self._scheduler = AsyncIOScheduler(jobstores=JOB_STORES)
        self._scheduler.start()

    def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        start_from: datetime,
        func: Any,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
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
        func: Any
            function to be triggered periodically
        args: Optional[List[Any]]
            args to func
        kwargs: Optional[Dict[str, Any]]
            kwargs to func
        """
        interval_trigger = IntervalTrigger(
            start_date=start_from, seconds=interval_seconds, timezone="utc"
        )
        self._scheduler.add_job(
            id=job_id,
            jobstore=self._job_store,
            func=func,
            args=args,
            kwargs=kwargs,
            trigger=interval_trigger,
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
        jobs = self._scheduler.get_jobs(jobstore=self._job_store)
        return [job.id for job in jobs]


class TileSchedulerFactory:
    """
    Factory for TileScheduler
    """

    tile_scheduler_map: Dict[str, TileScheduler] = {}

    @classmethod
    def get_instance(cls, job_store: Optional[str] = None) -> TileScheduler:
        """
        Retrieve corresponding TileScheduler instance

        Parameters
        ----------
        job_store: Optional[str]
            input job store name

        Returns
        -------
            TileScheduler instance based on input job_store
        """
        if not cls.tile_scheduler_map:
            for j_store in JOB_STORES:
                cls.tile_scheduler_map[j_store] = TileScheduler(job_store=j_store)

        if not job_store:
            job_store = "default"

        return cls.tile_scheduler_map[job_store]
