"""
FeatureByte Tile Scheduler
"""
from typing import Any, Callable, ClassVar, Dict, List, Optional

from datetime import datetime

from apscheduler.jobstores.base import JobLookupError
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, PrivateAttr

from featurebyte.logger import logger


class TileScheduler(BaseModel):
    """
    FeatureByte Scheduler using apscheduler
    """

    _job_stores: ClassVar[Dict[str, Any]] = {
        "local": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite"),
        # "mongo": MongoDBJobStore(),
    }

    # singleton scheduler
    _scheduler: ClassVar[BackgroundScheduler] = BackgroundScheduler(jobstores=_job_stores)
    _job_store: str = PrivateAttr()

    def __init__(self, job_store: str = "local", **kw: Any) -> None:
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

        try:
            self._scheduler.start()
        except SchedulerAlreadyRunningError:
            logger.warning("Scheduler is already running")

    def start_job_with_interval(
        self,
        job_id: str,
        interval_seconds: int,
        start_from: datetime,
        func: Callable[[Any], Any],
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Start job with cron expression

        Parameters
        ----------
        job_id: str
            job id
        interval_seconds: int
            interval between runs
        start_from: datetime
            starting point for the interval calculation
        func: Callable[[Any], Any]
            function to be triggered periodically
        args: Optional[List[Any]]
            args to func
        kwargs: Optional[Dict[str, Any]]
            kwargs to func
        """
        interval_trigger = IntervalTrigger(start_date=start_from, seconds=interval_seconds)
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

    def get_jobs(self) -> List[str]:
        """
        Get Jobs from input job store

        Returns
        ----------
            List of job ids
        """
        jobs = self._scheduler.get_jobs(jobstore=self._job_store)
        return [job.id for job in jobs]
