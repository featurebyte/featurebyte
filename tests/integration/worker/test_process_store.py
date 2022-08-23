"""
Tests for process store
"""
import json
from multiprocessing import Process

import pytest
from bson.objectid import ObjectId

from featurebyte.worker.process_store import ProcessStore
from tests.util.task import Command, TaskExecutor


@pytest.mark.asyncio
async def test_process_store():
    """Test process store"""
    # set class variable to use test command & test task executor
    ProcessStore._command_class = Command
    ProcessStore._task_executor = TaskExecutor

    # {user_id: {command: <command>, exitcode: <expected_exitcode>}}
    user_map = {
        ObjectId(): {"command": Command.LONG_RUNNING_COMMAND, "exitcode": 0, "status": "SUCCESS"},
        ObjectId(): {"command": Command.ERROR_COMMAND, "exitcode": 0, "status": "FAILURE"},
        None: {"command": Command.LONG_RUNNING_COMMAND, "exitcode": 0, "status": "SUCCESS"},
    }
    user_task_status_pid_map = {}
    process_data_list = []

    for user_id, info in user_map.items():
        task_status_id = await ProcessStore().submit(
            payload=json.dumps(
                {
                    "command": info["command"],
                    "output_document_id": str(ObjectId()),
                    "output_collection_name": "some_collection",
                    "user_id": str(user_id) if user_id else None,
                }
            ),
            output_path="some_output_path",
        )

        # test get
        process_data = await ProcessStore().get(user_id=user_id, task_status_id=task_status_id)
        process_data_list.append(process_data)
        # check process is running
        assert process_data["process"].exitcode is None
        user_task_status_pid_map[user_id] = (task_status_id, process_data["process"].pid)

    # test list
    for user_id in user_map.keys():
        task_status_id_process_pairs = await ProcessStore().list(user_id)
        assert len(task_status_id_process_pairs) == 1
        assert task_status_id_process_pairs[0][0] == user_task_status_pid_map[user_id][0]
        assert (
            task_status_id_process_pairs[0][1]["process"].pid
            == user_task_status_pid_map[user_id][1]
        )

    # wait processes finish
    for proc_data in process_data_list:
        proc_data["process"].join()

    # check process exitcode
    process_store = ProcessStore()
    for user_id, (task_status_id, _) in user_task_status_pid_map.items():
        process_data = await process_store.get(user_id=user_id, task_status_id=task_status_id)
        assert process_data["process"].exitcode == user_map[user_id]["exitcode"]
        assert process_data["status"] == user_map[user_id]["status"]
        assert process_data["output_path"] == "some_output_path"

        process_list = await process_store.list(user_id=user_id)
        assert len(process_list) == 1
        assert process_list[0][0] == task_status_id
