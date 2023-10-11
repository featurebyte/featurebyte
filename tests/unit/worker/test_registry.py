"""
Test registry
"""
from featurebyte.enum import WorkerCommand
from featurebyte.worker.registry import TASK_REGISTRY_MAP


def test_that_all_worker_command_enums_are_in_registry():
    """
    Test all worker command enums are in registry
    """
    assert len(TASK_REGISTRY_MAP) == len(WorkerCommand)
    # Verify that all WorkerCommand enums are covered in the TASK_REGISTRY_MAP
    for key in WorkerCommand:
        assert key in TASK_REGISTRY_MAP
