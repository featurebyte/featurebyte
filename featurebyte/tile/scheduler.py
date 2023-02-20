"""
FeatureByte Tile Scheduler
"""
from typing import Any, Callable, Dict, List, Optional

from datetime import datetime

from apscheduler.jobstores.base import JobLookupError
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, PrivateAttr

from featurebyte.logger import logger

JOB_STORES = {
    "local": SQLAlchemyJobStore(url="sqlite:///jobs.sqlite"),
    # "mongo": MongoDBJobStore(),
}

# singleton scheduler
SCHEDULER = BackgroundScheduler(jobstores=JOB_STORES)


class TileScheduler(BaseModel):
    """
    FeatureByte Scheduler using apscheduler
    """

    _scheduler: BackgroundScheduler = PrivateAttr()
    _jobstore: str = PrivateAttr()

    def __init__(self, jobstore: str = "local", **kw: Any) -> None:
        """
        Instantiate BackgroundScheduler instance

        Parameters
        ----------
        jobstore: str
            job store name
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._jobstore = jobstore
        self._scheduler = SCHEDULER
        try:
            self._scheduler.start()
        except SchedulerAlreadyRunningError:
            logger.warning("Scheduler is already running")

    def start_job_with_cron(
        self,
        job_id: str,
        cron: str,
        func: Callable[[Any], Any],
        args: Optional[List[Any]],
        kwargs: Optional[Dict[str, Any]],
    ) -> None:
        """
        Start job with cron expression

        Parameters
        ----------
        job_id: str
            job id
        cron: str
            cron expression
        func: Callable[[Any], Any]
            function to be triggered periodically
        args: Optional[List[Any]]
            args to func
        kwargs: Optional[Dict[str, Any]]
            kwargs to func
        """
        cron_trigger = CronTrigger.from_crontab(cron)
        self._scheduler.add_job(
            id=job_id,
            jobstore=self._jobstore,
            func=func,
            args=args,
            kwargs=kwargs,
            trigger=cron_trigger,
        )

    def start_job_with_interval(
        self,
        job_id: str,
        interval_minutes: int,
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
        interval_minutes: int
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
        interval_trigger = IntervalTrigger(start_date=start_from, minutes=interval_minutes)
        self._scheduler.add_job(
            id=job_id,
            jobstore=self._jobstore,
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
            self._scheduler.remove_job(job_id=job_id, jobstore=self._jobstore)
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
        jobs = self._scheduler.get_jobs(jobstore=self._jobstore)
        return [job.id for job in jobs]
