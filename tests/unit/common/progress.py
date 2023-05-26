"""
Unit tests for featurebyte.common.progress
"""
from unittest.mock import Mock, call

import pytest

from featurebyte.common.progress import get_ranged_progress_callback


@pytest.fixture(name="progress_callback")
def progress_callback_fixture():
    """
    Fixture for progress_callback
    """
    return Mock(name="mock_progress_callback")


def test_set_progress_range(progress_callback):
    """
    Test that get_ranged_progress_callback works as expected
    """
    new_callback = get_ranged_progress_callback(progress_callback, 10, 20)
    for i in [0, 50, 100]:
        new_callback(i, "Doing a subtask")
    assert progress_callback.call_args_list == [
        call(10, "Doing a subtask"),
        call(15, "Doing a subtask"),
        call(20, "Doing a subtask"),
    ]


def test_set_progress_range_nested(progress_callback):
    """
    Test that nested get_ranged_progress_callback calls work as expected
    """
    new_callback = get_ranged_progress_callback(
        progress_callback, 10, 20
    )  # covers the full range's 10% - 20%
    newer_callback = get_ranged_progress_callback(
        new_callback, 50, 100
    )  # covers the full range's 15% - 20%
    for i in [0, 50, 100]:
        newer_callback(i, "Doing a smaller subtask")
    assert progress_callback.call_args_list == [
        call(15, "Doing a smaller subtask"),
        call(17, "Doing a smaller subtask"),
        call(20, "Doing a smaller subtask"),
    ]
