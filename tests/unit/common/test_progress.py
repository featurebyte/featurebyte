"""
Unit tests for featurebyte.common.progress
"""

from unittest.mock import AsyncMock, call

import pytest

from featurebyte.common.progress import (
    get_ranged_progress_callback,
    ranged_progress_callback_iterator,
)


@pytest.fixture(name="progress_callback")
def progress_callback_fixture():
    """
    Fixture for progress_callback
    """
    return AsyncMock(name="mock_progress_callback")


@pytest.mark.asyncio
async def test_get_ranged_progress_callback(progress_callback):
    """
    Test that get_ranged_progress_callback works as expected
    """
    new_callback = get_ranged_progress_callback(progress_callback, 10, 20)
    for i in [0, 50, 100]:
        await new_callback(i, "Doing a subtask")
    assert progress_callback.call_args_list == [
        call(10, "Doing a subtask"),
        call(15, "Doing a subtask"),
        call(20, "Doing a subtask"),
    ]


@pytest.mark.asyncio
async def test_get_ranged_progress_callback__clip_start_threshold(progress_callback):
    """
    Test that get_ranged_progress_callback works as expected
    """
    new_callback = get_ranged_progress_callback(progress_callback, 0, 10, clip_start_threshold=5)
    for i in [5, 10, 50, 100]:
        await new_callback(i, "Doing a subtask")
    assert progress_callback.call_args_list == [
        call(0, "Doing a subtask"),  # effective: 5 * (10 - 0) / 100 + 0 = 0.5 %, clipped to 0 %
        call(0, "Doing a subtask"),  # effective: 10 * (10 - 0) / 100 + 0 = 1 %, clipped to 0 %
        call(5, "Doing a subtask"),  # effective: 50 * (10 - 0) / 100 + 0 = 5 %, not clipped
        call(10, "Doing a subtask"),  # effective: 100 * (10 - 0) / 100 + 0 = 10 %, not clipped
    ]


@pytest.mark.asyncio
async def test_get_ranged_progress_callback_nested(progress_callback):
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
        await newer_callback(i, "Doing a smaller subtask")
    assert progress_callback.call_args_list == [
        call(15, "Doing a smaller subtask"),
        call(17, "Doing a smaller subtask"),
        call(20, "Doing a smaller subtask"),
    ]


@pytest.mark.asyncio
async def test_ranged_progress_callback_iterator(progress_callback):
    """Test that ranged_progress_callback_iterator works as expected"""
    # check that when there are more than 100 items, things are still working
    items = list(range(200))
    for item, ranged_callback in ranged_progress_callback_iterator(items, progress_callback):
        await ranged_callback(100, "Processing item")

    assert progress_callback.call_args_list[:10] == [
        call(1, "Processing item"),
        call(1, "Processing item"),
        call(2, "Processing item"),
        call(2, "Processing item"),
        call(3, "Processing item"),
        call(3, "Processing item"),
        call(4, "Processing item"),
        call(4, "Processing item"),
        call(5, "Processing item"),
        call(5, "Processing item"),
    ]
