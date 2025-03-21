"""
Test for task manager service
"""

import datetime
import math
import time
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from celerybeatmongo.models import PeriodicTask as PeriodicTaskDoc

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import User
from featurebyte.models.periodic_task import Crontab, Interval
from featurebyte.models.task import Task
from featurebyte.schema.task import TaskStatus
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.test_util.random_task import LongRunningPayload


@pytest.fixture(name="celery")
def celery_fixture():
    """Celery fixture"""
    mock_celery = Mock()
    mock_celery.send_task.side_effect = lambda *args, **kwargs: Mock(id=uuid4())
    mock_celery.AsyncResult.return_value.status = TaskStatus.STARTED
    yield mock_celery


@pytest.fixture(name="task_manager")
def task_manager_fixture(user_id, persistent, celery, catalog, storage):
    """Task manager fixture"""
    user = User(id=user_id)
    task_manager = TaskManager(
        user=user,
        persistent=persistent,
        celery=celery,
        catalog_id=catalog.id,
        storage=storage,
        redis=Mock(),
    )
    yield task_manager


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
                "priority": 2,
                "is_scheduled_task": False,
                "is_revocable": True,
                "is_rerunnable": True,
            },
            queue="io_task:2",
            parent_id=None,
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
            queue="cpu_task:2",
        )
        document = task.model_dump(by_alias=True)
        document["_id"] = str(document["_id"])
        await persistent._db[Task.collection_name()].replace_one({"_id": task_id}, document)

        task = await task_manager.get_task(task_id=task_id)
        assert str(task.id) == task_id
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
        task_ids.append(UUID(task_id))
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
            queue="cpu_task:2",
        )
        document = task.model_dump(by_alias=True)
        document["_id"] = str(document["_id"])
        await persistent._db[Task.collection_name()].replace_one({"_id": task_id}, document)
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

    # check list with filter
    selected_task_ids = [str(task_id) for task_id in task_ids[:2]]
    items, total = await task_manager.list_tasks(
        page=1,
        page_size=10,
        query_filter={"_id": {"$in": selected_task_ids}},
    )
    assert total == 2
    assert {str(item.id) for item in items} == set(selected_task_ids)


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
    assert periodic_task.kwargs == {**payload.json_dict(), "is_scheduled_task": True}
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
    crontab = Crontab(minute="*/1", hour=0, day_of_week="*", day_of_month="*", month_of_year="*")
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
    assert periodic_task.kwargs == {**payload.json_dict(), "is_scheduled_task": True}
    assert periodic_task.crontab == Crontab(**{**crontab.model_dump(), "hour": "0"})

    # check that celerybeatmongo PeriodicTaskDoc model is created without any error
    periodic_task_doc = PeriodicTaskDoc(**periodic_task.model_dump())
    assert periodic_task_doc.crontab.hour == "0"

    await task_manager.delete_periodic_task(periodic_task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(periodic_task_id)


@pytest.mark.asyncio
@pytest.mark.disable_task_manager_mock
async def test_task_manager__revoke_tasks(task_manager, celery, user_id, persistent, catalog):
    """Test task manager service"""
    payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
    task_id = await task_manager.submit(payload=payload)

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
        queue="cpu_task:2",
    )
    document = task.model_dump(by_alias=True)
    document["_id"] = str(document["_id"])
    document["child_task_ids"] = [str(uuid4()), str(uuid4()), str(uuid4())]
    await persistent._db[Task.collection_name()].replace_one({"_id": task_id}, document)

    task_manager.redis.lrange.return_value = None

    # revoke task
    await task_manager.revoke_task(task_id)

    # check celery task revoke calls
    task_ids = [task_id] + document["child_task_ids"]
    expected_kwargs = {"reply": True, "terminate": True, "signal": "SIGTERM"}
    for i, task_id in enumerate(task_ids):
        assert celery.control.revoke.call_args_list[i].args == (task_id,)
        assert celery.control.revoke.call_args_list[i].kwargs == expected_kwargs


@pytest.mark.asyncio
@pytest.mark.disable_task_manager_mock
async def test_task_manager__submit_mark_as_scheduled_task(task_manager, celery, user_id, catalog):
    """
    Test submitting task and marking the task as scheduled task
    """
    payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
    await task_manager.submit(
        payload=payload,
        mark_as_scheduled_task=True,
    )
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
            "priority": 2,
            "is_scheduled_task": True,  # check this is set
            "is_revocable": True,
            "is_rerunnable": True,
        },
        queue="io_task:2",
        parent_id=None,
    )


@pytest.mark.asyncio
@pytest.mark.disable_task_manager_mock
async def test_task_manager__rerun_task(task_manager, celery, user_id, persistent, catalog):
    """
    Test rerunning task
    """
    payload = LongRunningPayload(user_id=user_id, catalog_id=catalog.id)
    task_id = await task_manager.submit(payload=payload)

    # insert task into db manually since we are mocking celery
    task = Task(
        _id=task_id,
        status=TaskStatus.FAILURE,
        result="",
        children=[],
        date_done=datetime.datetime.utcnow(),
        name=LongRunningPayload.command,
        args=[],
        kwargs=celery.send_task.call_args.kwargs["kwargs"],
        worker="worker",
        retries=0,
        queue="cpu_task:2",
    )
    document = task.model_dump(by_alias=True)
    document["_id"] = str(document["_id"])
    await persistent._db[Task.collection_name()].replace_one({"_id": task_id}, document)

    new_task_id = await task_manager.rerun_task(task_id)
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
            "priority": 2,
            "is_revocable": True,
            "is_rerunnable": True,
            "is_scheduled_task": False,
        },
        queue="cpu_task:2",
    )

    assert new_task_id != task_id
