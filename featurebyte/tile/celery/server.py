"""
Start up embedded Redis server instance and Celery beat & worker instance
"""
from typing import Optional, cast

import subprocess

import redis
import redislite
from pydantic import BaseModel, Field

from featurebyte.logger import logger


class FBRedis(BaseModel):
    """
    Redis Server
    """

    redis_port: int
    redis_host: str
    redis_server: Optional[redislite.Redis] = Field(default=None)
    redis_pid: int = Field(default=-1)
    redis_pid_key: str = Field(default="redis_pid")

    class Config:
        arbitrary_types_allowed = True

    @property
    def redis_url(self) -> str:
        """
        property for Redis URL

        Returns
        -------
            redis url
        """
        return f"redis://{self.redis_host}:{self.redis_port}"

    @property
    def redis_conn(self) -> redis.Redis:  # type: ignore
        """
        property for Redis connection instance

        Returns
        -------
            Redis connection instance
        """
        return redis.Redis(host=self.redis_host, port=self.redis_port)

    def startup(self) -> None:
        """
        Start up embedded redis server is not started
        """
        try:
            redis_pid = self.redis_conn.get(self.redis_pid_key)
            logger.info(f"existing redis_pid: {redis_pid}")
        except redis.exceptions.ConnectionError:
            logger.info(f"Starting local redis {self.redis_url}")
            self.redis_server = redislite.Redis(serverconfig={"port": self.redis_port})
            redis_pid = self.redis_server.pid

            self.redis_conn.set(self.redis_pid_key, redis_pid)
            logger.info(f"new redis_pid: {redis_pid}")

        self.redis_pid = cast(int, redis_pid)

    def shutdown(self) -> None:
        """
        Shutdown the redis sever
        """
        if self.redis_server:
            self.redis_conn.delete(self.redis_pid_key)
            self.redis_server.shutdown()


class FBCelery(BaseModel):
    """
    Celery server instance
    """

    redis_server: FBRedis
    celery_pid: int = Field(default=-1)
    celery_pid_key: str = Field(default="celery_pid")

    def startup(self) -> None:
        """
        Start up celery server is not started
        """
        self.celery_pid = cast(int, self.redis_server.redis_conn.get(self.celery_pid_key))
        logger.info(f"existing celery_pid: {self.celery_pid}")
        if not self.celery_pid:
            command = "celery -A featurebyte.tile.celery.tasks worker --beat --loglevel=DEBUG --scheduler redbeat.RedBeatScheduler"
            process = subprocess.Popen(
                command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, shell=True
            )
            self.celery_pid = process.pid
            self.redis_server.redis_conn.set(self.celery_pid_key, self.celery_pid)
            logger.info(f"new celery_pid: {self.celery_pid}")

    def shutdown(self) -> None:
        """
        Shutdown both redis and celery sever
        """
        if self.redis_server:
            self.redis_server.redis_conn.delete(self.celery_pid_key)
            subprocess.Popen(f"kill {self.celery_pid}", shell=True)
            self.redis_server.shutdown()


redis_server = FBRedis(redis_port=6379, redis_host="localhost")
redis_server.startup()

celery_instance = FBCelery(redis_server=redis_server)
celery_instance.startup()
