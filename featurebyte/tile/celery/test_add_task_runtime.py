"""
Test Class for Dynamically adding Celery Periodic class using RedBeat
"""
from datetime import datetime

from celery.schedules import schedule
from pydantic.main import BaseModel
from redbeat import RedBeatSchedulerEntry

from featurebyte.tile.celery.tasks import celery_app


class TestCommon(BaseModel):
    """
    Simulated class for Tile Operation Classes
    """

    async def execute(self) -> None:
        """
        Execute tile generate operation
        """
        with open("test.txt", "a") as myfile:
            myfile.write(str(datetime.now()) + "\n")


interval = schedule(run_every=5)

test_ins = TestCommon()

entry2 = RedBeatSchedulerEntry(
    name="test-redbeat-2",
    task="featurebyte.tile.celery.tasks.execute",
    schedule=interval,
    args=["featurebyte.tile.celery.test_add_task_runtime", "TestCommon", test_ins.json()],
    app=celery_app,
)
entry2.save()
