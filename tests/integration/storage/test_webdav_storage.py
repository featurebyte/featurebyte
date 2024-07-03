"""
Test S3Storage class
"""

import pytest
import pytest_asyncio
from python_on_whales import docker

from featurebyte.storage.webdav import WebdavStorage
from tests.integration.storage.base import BaseStorageTestSuite


class TestWebdavStorage(BaseStorageTestSuite):
    """
    Test suite for S3Storage class
    """

    @pytest.fixture(name="rclone", scope="session")
    def setup(self, request) -> str:
        """
        Setup rclone server
        """
        url = "http://localhost:10079"
        rclone_docker = docker.run(
            "rclone/rclone:latest",
            name="rclone-test",
            remove=True,
            detach=True,
            publish=[("10079", "10079")],
            command=["serve", "webdav", "/tmp", "--addr", ":10079"],
        )
        yield url
        rclone_docker.stop()

    @pytest_asyncio.fixture(name="test_storage")
    async def storage_fixture(self, rclone: str):
        """
        Webdav fixture
        """

        storage = WebdavStorage(base_url=rclone)
        yield storage
