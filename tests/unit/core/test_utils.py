"""
Test helper functions in featurebyte.core.utils
"""
import pytest

from featurebyte.core.utils import run_async


async def sync_test_func(arg1: str, arg2: str):
    """
    Async function for testing
    """
    return f"Received arg1: {arg1}, arg2: {arg2}"


def test_run_async_non_async():
    """
    Run run_async in non async context
    """
    assert run_async(sync_test_func, "apple", arg2="orange") == "Received arg1: apple, arg2: orange"


@pytest.mark.asyncio
async def test_run_async_async():
    """
    Run run_async in async context
    """
    assert run_async(sync_test_func, "apple", arg2="orange") == "Received arg1: apple, arg2: orange"
