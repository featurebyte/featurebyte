"""
Unit tests for worker/util directory
"""

from unittest.mock import Mock, patch

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

    progress = Mock()
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
    assert updated_doc["progress_history"] == {"data": [[50, "Test progress"]]}

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
            [91, "Second test progress 91"],
            [92, "Second test progress 92"],
            [93, "Second test progress 93"],
            [94, "Second test progress 94"],
            [95, "Second test progress 95"],
            [96, "Second test progress 96"],
            [97, "Second test progress 97"],
            [98, "Second test progress 98"],
            [99, "Second test progress 99"],
            [100, "Second test progress 100"],
        ],
    }
    assert updated_doc["progress"] == {"percent": 100, "message": "Second test progress 100"}
