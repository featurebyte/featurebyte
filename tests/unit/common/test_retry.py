"""
Test retry module
"""
# pylint: disable=broad-exception-raised
import pytest

from featurebyte.common.retry import async_retry


@pytest.mark.asyncio
async def test_async_retry__succeeded():
    """Test async_retry (success)"""

    @async_retry()
    async def success_func():
        return True

    output = await success_func()
    assert output is True


@pytest.mark.asyncio
async def test_async_retry__first_failed_and_later_succeeded():
    """Test async_retry (first failed and later succeeded)"""

    @async_retry(max_retries=1, backoff_factor=0)
    async def failed_and_later_succeeded_func(cnt):
        if cnt[0] < 1:
            cnt[0] += 1
            raise Exception("failed")
        return True

    counter = [0]
    output = await failed_and_later_succeeded_func(counter)
    assert output is True
    assert counter == [1]


@pytest.mark.asyncio
async def test_async_retry__failed():
    """Test async_retry (failed)"""

    @async_retry(max_retries=1, backoff_factor=0)
    async def failed_func(cnt):
        cnt[0] += 1
        raise Exception("failed")

    counter = [0]
    with pytest.raises(Exception):
        await failed_func(counter)

    assert counter == [2]
