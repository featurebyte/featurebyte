"""
Test S3Storage class
"""

import os
from unittest.mock import patch

import pytest_asyncio
from bson import ObjectId

from featurebyte.storage import S3Storage
from featurebyte.utils.storage import get_client
from tests.integration.storage.base import BaseStorageTestSuite


class TestS3StorageSuite(BaseStorageTestSuite):
    """
    Test suite for S3Storage class
    """

    @pytest_asyncio.fixture(name="test_storage")
    async def storage_fixture(self):
        """
        Storage object fixture
        """

        with (
            patch("featurebyte.utils.storage.S3_URL", "https://s3.ap-southeast-1.amazonaws.com"),
            patch(
                "featurebyte.utils.storage.S3_ACCESS_KEY_ID",
                os.environ["S3_ACCESS_KEY"],
            ),
            patch(
                "featurebyte.utils.storage.S3_SECRET_ACCESS_KEY",
                os.environ["S3_SECRET_KEY"],
            ),
        ):
            bucket_name = "featurebyte-cicd"
            prefix = str(ObjectId())
            yield S3Storage(get_client=get_client, bucket_name=bucket_name, prefix=prefix)

            # cleanup remote folder
            async with get_client() as client:
                response = await client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                if "Contents" in response:
                    objects_to_clean = [obj["Key"] for obj in response["Contents"]]
                    for key in objects_to_clean:
                        await client.delete_object(
                            Bucket=bucket_name,
                            Key=key,
                        )
