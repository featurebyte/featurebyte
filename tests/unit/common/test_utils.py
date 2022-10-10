"""
Test helper functions in featurebyte.common.utils
"""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import (
    dataframe_from_arrow_stream,
    dataframe_to_arrow_bytes,
    run_async,
)


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


def test_dataframe_to_arrow_bytes():
    """
    Test dataframe_to_arrow_bytes
    """
    original_df = pd.DataFrame(
        {
            "a": range(10),
            "b": range(10),
        }
    )
    data = dataframe_to_arrow_bytes(original_df)
    output_df = dataframe_from_arrow_stream(data)
    assert_frame_equal(output_df, original_df)
