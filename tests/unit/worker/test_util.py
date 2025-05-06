"""
Unit tests for worker/util directory
"""

from unittest.mock import AsyncMock, patch

import pytest
from bson import ObjectId

from featurebyte.models.base import User
from featurebyte.models.task import Task
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


@pytest.mark.asyncio
async def test_progress_update(persistent, user_id):
    """Test progress update"""
    task_id = str(ObjectId())

    # create a task document
    await persistent.insert_one(
        collection_name=Task.collection_name(),
        document={
            "_id": task_id,
            "user_id": user_id,
            "description": "Test task",
            "start_time": None,
            "end_time": None,
            "status": "running",
        },
        user_id=user_id,
        disable_audit=True,
    )

    progress = AsyncMock()
    progress_updater = TaskProgressUpdater(
        persistent=persistent, task_id=task_id, user=User(id=user_id), progress=progress
    )
    await progress_updater.update_progress(
        percent=50, message="Test progress", metadata={"task_specific_value": "1234"}
    )

    # check task document
    updated_doc = await persistent.find_one(
        collection_name=Task.collection_name(),
        query_filter={"_id": task_id},
    )
    expected = {
        "percent": 50,
        "message": "Test progress",
        "metadata": {"task_specific_value": "1234"},
    }
    assert updated_doc["progress"] == expected
    assert updated_doc["progress_history"] == {
        "data": [
            {
                "percent": 50,
                "message": "Test progress",
                "timestamp": updated_doc["progress_history"]["data"][0]["timestamp"],
            }
        ],
    }

    # check progress history compression logic
    with patch.object(TaskProgressUpdater, "max_progress_history", 10):
        for i in range(50, 101):
            await progress_updater.update_progress(percent=i, message=f"First test progress {i}")
            await progress_updater.update_progress(percent=i, message=f"Second test progress {i}")

    # check task document
    updated_doc = await persistent.find_one(
        collection_name=Task.collection_name(),
        query_filter={"_id": task_id},
    )
    assert updated_doc["progress_history"] == {
        "compress_at": 100,
        "data": [
            {
                "percent": 91,
                "message": "Second test progress 91",
                "timestamp": updated_doc["progress_history"]["data"][0]["timestamp"],
            },
            {
                "percent": 92,
                "message": "Second test progress 92",
                "timestamp": updated_doc["progress_history"]["data"][1]["timestamp"],
            },
            {
                "percent": 93,
                "message": "Second test progress 93",
                "timestamp": updated_doc["progress_history"]["data"][2]["timestamp"],
            },
            {
                "percent": 94,
                "message": "Second test progress 94",
                "timestamp": updated_doc["progress_history"]["data"][3]["timestamp"],
            },
            {
                "percent": 95,
                "message": "Second test progress 95",
                "timestamp": updated_doc["progress_history"]["data"][4]["timestamp"],
            },
            {
                "percent": 96,
                "message": "Second test progress 96",
                "timestamp": updated_doc["progress_history"]["data"][5]["timestamp"],
            },
            {
                "percent": 97,
                "message": "Second test progress 97",
                "timestamp": updated_doc["progress_history"]["data"][6]["timestamp"],
            },
            {
                "percent": 98,
                "message": "Second test progress 98",
                "timestamp": updated_doc["progress_history"]["data"][7]["timestamp"],
            },
            {
                "percent": 99,
                "message": "Second test progress 99",
                "timestamp": updated_doc["progress_history"]["data"][8]["timestamp"],
            },
            {
                "percent": 100,
                "message": "Second test progress 100",
                "timestamp": updated_doc["progress_history"]["data"][9]["timestamp"],
            },
        ],
    }
    assert updated_doc["progress"] == {"percent": 100, "message": "Second test progress 100"}
