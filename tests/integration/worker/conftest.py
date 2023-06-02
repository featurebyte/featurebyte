"""
Common test fixtures used across files in worker directory
"""
from typing import Any

import os
import subprocess
import threading

import pymongo
import pytest_asyncio
from bson import ObjectId

from featurebyte.persistent.mongo import MongoDB
from featurebyte.worker import celery
from tests.integration.conftest import MONGO_CONNECTION


class RunThread(threading.Thread):
    """
    Thread to execute query
    """

    def __init__(self, stdout: Any) -> None:
        self.stdout = stdout
        super().__init__()

    def run(self) -> None:
        """
        Run async function
        """
        print(self.stdout.read().decode("utf-8"))


@pytest_asyncio.fixture(scope="session", name="celery_service")
async def celery_service_fixture():
    """
    Start celery service for testing
    """
    # create new database for testing
    database_name = f"test_{ObjectId()}"
    client = pymongo.MongoClient(MONGO_CONNECTION)
    persistent = MongoDB(uri=MONGO_CONNECTION, database=database_name)
    env = os.environ.copy()
    env.update({"MONGODB_URI": MONGO_CONNECTION, "MONGODB_DB": database_name})
    celery.conf.mongodb_backend_settings["database"] = database_name
    proc = subprocess.Popen(
        [
            "celery",
            "--app",
            "featurebyte.worker.start.celery",
            "worker",
            "-Q",
            "cpu_task,cpu_task:1,cpu_task:2,cpu_task:3",
            "--loglevel=INFO",
            "--beat",
            "--scheduler",
            "celerybeatmongo.schedulers.MongoScheduler",
        ],
        env=env,
        stdout=subprocess.PIPE,
    )
    thread = RunThread(proc.stdout)
    thread.daemon = True
    thread.start()
    yield persistent
    proc.terminate()
    client.drop_database(database_name)
