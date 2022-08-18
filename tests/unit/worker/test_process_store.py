"""
Tests for process store
"""
import time
from multiprocessing import Process

from bson.objectid import ObjectId

from featurebyte.worker.process_store import ProcessStore
from tests.unit.worker.task import Command, TaskExecutor


def test_process_store():
    """Test process store"""
    # set class variable to use test command & test task executor
    ProcessStore._command_class = Command
    ProcessStore._task_executor = TaskExecutor

    # {user_id: {command: <command>, exitcode: <expected_exitcode>}}
    user_map = {
        ObjectId(): {"command": Command.LONG_RUNNING_COMMAND, "exitcode": 0},
        ObjectId(): {"command": Command.ERROR_COMMAND, "exitcode": 1},
    }
    user_task_status_pid_map = {}

    for user_id, info in user_map.items():
        task_status_id = ProcessStore().submit(
            payload={
                "command": info["command"],
                "document_id": ObjectId(),
                "collection_name": "some_collection",
                "user_id": user_id,
            }
        )

        # test get
        process = ProcessStore().get(user_id=user_id, task_status_id=task_status_id)
        assert isinstance(process, Process)
        # check process is running
        assert process.exitcode is None
        user_task_status_pid_map[user_id] = (task_status_id, process.pid)

    # test list
    for user_id in user_map.keys():
        task_status_id_process_pairs = ProcessStore().list(user_id)
        assert len(task_status_id_process_pairs) == 1
        assert task_status_id_process_pairs[0][0] == user_task_status_pid_map[user_id][0]
        assert task_status_id_process_pairs[0][1].pid == user_task_status_pid_map[user_id][1]

    # wait processes finish
    time.sleep(2)

    # check process exitcode
    for user_id, (task_status_id, _) in user_task_status_pid_map.items():
        process = ProcessStore().get(user_id=user_id, task_status_id=task_status_id)
        assert isinstance(process, Process)
        assert process.exitcode == user_map[user_id]["exitcode"]
