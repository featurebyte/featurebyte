"""This module contains unit tests for the batch creator module."""

import pytest

from featurebyte.api.api_object import ApiObject
from featurebyte.api.event_table import EventTable
from featurebyte.worker.util.batch_feature_creator import patch_api_object_cache


@pytest.mark.asyncio
async def test_patch_api_object_cache__async_func(snowflake_event_table):
    """Test patch_api_object_cache."""

    @patch_api_object_cache()
    async def check_api_object_cache():
        api_cache = ApiObject._cache
        assert len(api_cache) == 0
        assert api_cache.maxsize == 1024
        assert api_cache.ttl == 7200

        _ = EventTable.get_by_id(snowflake_event_table.id)
        assert len(api_cache) > 0  # check that the cache is populated

    # make the first call to the function to populate the cache during the execution of the function
    await check_api_object_cache()

    # check that the cache after the execution of the function
    assert ApiObject._cache.maxsize == 1024
    assert ApiObject._cache.ttl == 1

    # check that the cache is cleared after the execution of the function
    await check_api_object_cache()
