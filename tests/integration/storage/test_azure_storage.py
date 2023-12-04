"""
Test AzureBlobStorage class
"""
import os
from unittest.mock import patch

import pytest_asyncio
from bson import ObjectId

from featurebyte.storage import AzureBlobStorage
from featurebyte.utils.storage import get_azure_storage_blob_client
from tests.integration.storage.base import BaseStorageTestSuite


class TestAzureBlobStorageSuite(BaseStorageTestSuite):
    """
    Test suite for AzureBlobStorage class
    """

    @pytest_asyncio.fixture(name="test_storage")
    async def storage_fixture(self):
        """
        Storage object fixture
        """

        with patch(
            "featurebyte.utils.storage.AZURE_STORAGE_ACCOUNT_NAME",
            os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
        ), patch(
            "featurebyte.utils.storage.AZURE_STORAGE_ACCOUNT_KEY",
            os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        ), patch(
            "featurebyte.utils.storage.AZURE_STORAGE_CONTAINER_NAME",
            "storage-test",
        ):
            prefix = str(ObjectId())
            yield AzureBlobStorage(get_client=get_azure_storage_blob_client, prefix=prefix)

            # cleanup remote folder
            async with get_azure_storage_blob_client() as client:
                async for blob in client.list_blobs():
                    await client.delete_blob(blob.name, delete_snapshots="include")
