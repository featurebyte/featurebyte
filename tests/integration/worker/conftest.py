"""
Common test fixtures used across files in worker directory
"""

import os
import subprocess
import tempfile
import threading
import time
from typing import Any

import pymongo
import pytest
from bson import ObjectId

from featurebyte.app import get_celery
from featurebyte.persistent.mongo import MongoDB
from tests.integration.conftest import MONGO_CONNECTION, TEST_REDIS_URI


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


@pytest.fixture(name="worker_type", scope="module", params=["cpu", "io"])
def source_type_fixture(request):
    return request.param


@pytest.yield_fixture(scope="module", name="celery_service")
def celery_service_fixture(worker_type):
    if worker_type == "cpu":
        command = [
            "celery",
            "--app",
            "featurebyte.worker.start.celery",
            "worker",
            "-Q",
            "cpu_task,cpu_task:1,cpu_task:2,cpu_task:3",
            "--loglevel=INFO",
            "--beat",
            "--scheduler",
            "featurebyte.worker.schedulers.MongoScheduler",
        ]
    else:
        command = [
            "celery",
            "--app",
            "featurebyte.worker.start.celery",
            "worker",
            "-Q",
            "io_task,io_task:1,io_task:2,io_task:3",
            "--loglevel=INFO",
            "--pool=gevent",
            "-c",
            "500",
        ]

    with tempfile.TemporaryDirectory() as tempdir:
        # create new database for testing
        database_name = f"test_{ObjectId()}"
        client = pymongo.MongoClient(MONGO_CONNECTION)
        persistent = MongoDB(uri=MONGO_CONNECTION, database=database_name)
        env = os.environ.copy()
        env.update({
            "MONGODB_URI": MONGO_CONNECTION,
            "MONGODB_DB": database_name,
            "FEATUREBYTE_SERVER": "http://127.0.0.1:8080",
            "FEATUREBYTE_HOME": tempdir,
            "REDIS_URI": TEST_REDIS_URI,
        })
        celery = get_celery(redis_uri=TEST_REDIS_URI, mongo_uri=MONGO_CONNECTION)
        celery.conf.mongodb_backend_settings["database"] = database_name
        proc = subprocess.Popen(command, env=env, stdout=subprocess.PIPE)
        thread = RunThread(proc.stdout)
        thread.daemon = True
        thread.start()
        yield persistent, celery
        time.sleep(0.1)  # wait for job completion to be acknowledged
        proc.terminate()
        client.drop_database(database_name)
