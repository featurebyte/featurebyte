"""
Base class for storage testing
"""
import filecmp
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from featurebyte.storage import Storage


class BaseStorageTestSuite:
    """
    BaseStorageTestSuite class
    """

    @pytest.fixture(name="local_path")
    def local_path_fixture(self):
        """
        Path to local temporary file
        """
        with tempfile.NamedTemporaryFile(mode="w") as file_obj:
            file_obj.write("There is some content in this file")
            file_obj.flush()
            yield file_obj.name

    @pytest_asyncio.fixture(name="remote_path")
    async def remote_path_fixture(self, storage: Storage, local_path: Path):
        """
        Yield remote path of uploaded file
        """
        remote_path = "some/file"
        await storage.put(local_path, remote_path=remote_path)
        yield remote_path

    @pytest.mark.asyncio
    async def test_put_file_success(self, local_path: Path, remote_path: str):
        """
        Test file upload
        """
        # upload should work
        assert remote_path

    @pytest.mark.asyncio
    async def test_put_file_fail(self, local_path: Path, remote_path: str, storage: Storage):
        """
        Test file upload
        """
        # upload should fail if remote file already exist
        with pytest.raises(FileExistsError) as exc_info:
            await storage.put(local_path, remote_path=remote_path)
        assert str(exc_info.value) == "File already exists on remote path"

    @pytest.mark.asyncio
    async def test_get_file_success(self, local_path: Path, remote_path: str, storage: Storage):
        """
        Test file upload
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            # download should work
            await storage.get(remote_path, file_obj.name)

            # contents should match
            assert filecmp.cmp(local_path, file_obj.name)

    @pytest.mark.asyncio
    async def test_get_file_fail(self, storage: Storage):
        """
        Test file upload
        """
        with tempfile.NamedTemporaryFile() as file_obj:

            # download should fail
            with pytest.raises(FileNotFoundError) as exc_info:
                await storage.get("non/existent/path", file_obj.name)
            assert str(exc_info.value) == "Remote file does not exist"

    @pytest.mark.asyncio
    async def test_stream_file_success(self, local_path: Path, remote_path: str, storage: Storage):
        """
        Test file upload
        """
        file_stream = storage.get_file_stream(remote_path, chunk_size=5)
        read_bytes = b"".join([chunk async for chunk in file_stream])
        assert read_bytes.decode("utf-8") == "There is some content in this file"

    @pytest.mark.asyncio
    async def test_stream_file_fail(self, storage: Storage):
        """
        Test file upload
        """
        # get stream should fail
        with pytest.raises(FileNotFoundError) as exc_info:
            file_stream = storage.get_file_stream("non/existent/path", chunk_size=5)
            b"".join([chunk async for chunk in file_stream])

        assert str(exc_info.value) == "Remote file does not exist"
