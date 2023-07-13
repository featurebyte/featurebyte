"""
Test for task manager service
"""
import datetime
import math
import time
from unittest.mock import Mock
from uuid import uuid4

import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import User
from featurebyte.models.periodic_task import Crontab, Interval
from featurebyte.models.task import Task
from featurebyte.schema.task import TaskStatus
from featurebyte.service.task_manager import TaskManager
from tests.util.task import LongRunningPayload


@pytest.fixture(name="user_id")
def user_id_fixture():
    """User ID fixture"""
    return ObjectId()


@pytest.fixture(name="celery")
def celery_fixture():
    """Celery fixture"""
    mock_celery = Mock()
    mock_celery.send_task.side_effect = lambda *args, **kwargs: Mock(id=uuid4())
    mock_celery.AsyncResult.return_value.status = TaskStatus.STARTED
    yield mock_celery


@pytest.fixture(name="task_manager")
def task_manager_fixture(user_id, persistent, celery, catalog):
    """Task manager fixture"""
    yield TaskManager(
        user=User(id=user_id), persistent=persistent, celery=celery, catalog_id=catalog.id
    )


@pytest.mark.asyncio
@pytest.mark.disable_task_manager_mock
async def test_task_manager__long_running_tasks(task_manager, celery, user_id, persistent, catalog):
    """Test task manager service"""
    expected_tasks = []
    for _ in range(3):
        payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
        task_id = await task_manager.submit(payload=payload)

        # check celery task submission
        celery.send_task.assert_called_with(
            payload.task,
            kwargs={
                "user_id": str(user_id),
                "output_document_id": str(payload.output_document_id),
                "command": payload.command,
                "catalog_id": str(payload.catalog_id),
                "output_collection_name": payload.output_collection_name,
                "task_output_path": payload.task_output_path,
                "task_type": "io_task",
                "priority": 0,
            },
        )

        # insert task into db manually since we are mocking celery
        task = Task(
            _id=task_id,
            status=TaskStatus.STARTED,
            result="",
            children=[],
            date_done=datetime.datetime.utcnow(),
            name=LongRunningPayload.command,
            args=[],
            kwargs=celery.send_task.call_args.kwargs["kwargs"],
            worker="worker",
            retries=0,
            queue="default",
        )
        document = task.dict(by_alias=True)
        document["_id"] = str(document["_id"])
        await persistent._db[Task.collection_name()].insert_one(document)

        task = await task_manager.get_task(task_id=task_id)
        assert task.id == task_id
        assert task.status == "STARTED"
        expected_tasks.append(task)
        tasks, _ = await task_manager.list_tasks()
        assert tasks == expected_tasks
        time.sleep(0.1)


@pytest.mark.asyncio
@pytest.mark.disable_task_manager_mock
async def test_task_manager__list_tasks(task_manager, celery, user_id, persistent, catalog):
    """Test task manager service -- list task status"""
    task_num = 10
    task_ids = []
    for _ in range(task_num):
        task_id = await task_manager.submit(
            payload=LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
        )
        task_ids.append(task_id)
        # insert task into db manually since we are mocking celery
        task = Task(
            _id=task_id,
            status=TaskStatus.STARTED,
            result="",
            children=[],
            date_done=datetime.datetime.utcnow(),
            name=LongRunningPayload.command,
            args=[],
            kwargs=celery.send_task.call_args.kwargs["kwargs"],
            worker="worker",
            retries=0,
            queue="default",
        )
        document = task.dict(by_alias=True)
        document["_id"] = str(document["_id"])
        await persistent._db[Task.collection_name()].insert_one(document)
        time.sleep(0.1)

    page_sizes = [1, 2, 5, 10, 20]
    for page_size in page_sizes:
        total_page = math.ceil(task_num / page_size)
        ascending_list = []
        descending_list = []
        for page in range(1, total_page + 1):
            items, total = await task_manager.list_tasks(
                page=page, page_size=page_size, ascending=True
            )
            assert total == task_num
            ascending_list.extend(items)

            items, total = await task_manager.list_tasks(
                page=page, page_size=page_size, ascending=False
            )
            assert total == task_num
            descending_list.extend(items)

        # check list order
        assert [item.id for item in ascending_list] == task_ids
        assert [item.id for item in descending_list] == list(reversed(task_ids))


@pytest.mark.asyncio
async def test_task_manager__schedule_interval_task(task_manager, user_id, catalog):
    """
    Test task manager service -- schedule interval task
    """
    interval = Interval(every=1, period="minutes")
    payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
    periodic_task_id = await task_manager.schedule_interval_task(
        name="test_interval_task",
        payload=payload,
        interval=interval,
    )
    periodic_task = await task_manager.get_periodic_task(
        periodic_task_id,
    )
    assert periodic_task.name == "test_interval_task"
    assert periodic_task.kwargs == payload.json_dict()
    assert periodic_task.interval == interval
    assert periodic_task.soft_time_limit == 60

    await task_manager.delete_periodic_task(periodic_task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(periodic_task_id)


@pytest.mark.asyncio
async def test_task_manager__schedule_cron_task(task_manager, user_id, catalog):
    """
    Test task manager service -- schedule interval task
    """
    crontab = Crontab(minute="*/1", hour="*", day_of_week="*", day_of_month="*", month_of_year="*")
    payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
    periodic_task_id = await task_manager.schedule_cron_task(
        name="test_cron_task",
        payload=payload,
        crontab=crontab,
    )
    periodic_task = await task_manager.get_periodic_task(
        periodic_task_id,
    )
    assert periodic_task.name == "test_cron_task"
    assert periodic_task.kwargs == payload.json_dict()
    assert periodic_task.crontab == crontab

    await task_manager.delete_periodic_task(periodic_task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(periodic_task_id)
