"""
Base class for storage testing
"""
import filecmp
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio
from pydantic import BaseModel

from featurebyte.storage import Storage


class BaseStorageTestSuite:
    """
    BaseStorageTestSuite class
    """

    @pytest.fixture(name="pydantic_object")
    def pydantic_object_fixture(self):
        """
        Pydantic object
        """

        class SomeModel(BaseModel):
            """
            Test pydantic class
            """

            text: str
            value: int

        return SomeModel(text="Some text value", value=1234)

    @pytest.fixture(name="text_file_content")
    def text_file_content_fixture(self):
        """
        Content of text file
        """
        return "There is some content in this file"

    @pytest.fixture(name="local_path")
    def local_path_fixture(self, text_file_content):
        """
        Path to local temporary file
        """
        with tempfile.NamedTemporaryFile(mode="w") as file_obj:
            file_obj.write(text_file_content)
            file_obj.flush()
            yield file_obj.name

    @pytest_asyncio.fixture(name="remote_path")
    async def remote_path_fixture(self, test_storage: Storage, local_path: Path):
        """
        Yield remote path of uploaded file
        """
        remote_path = Path("some/file")
        await test_storage.put(local_path, remote_path=remote_path)
        yield remote_path

    @pytest.mark.asyncio
    async def test_put_file_success(self, remote_path: str):
        """
        Test file upload
        """
        # upload should work
        assert remote_path

    @pytest.mark.asyncio
    async def test_put_file_fail(self, local_path: Path, remote_path: Path, test_storage: Storage):
        """
        Test file upload
        """
        # upload should fail if remote file already exist
        with pytest.raises(FileExistsError) as exc_info:
            await test_storage.put(local_path, remote_path=remote_path)
        assert str(exc_info.value) == "File already exists on remote path"

    @pytest.mark.asyncio
    async def test_get_file_success(
        self, local_path: Path, remote_path: Path, test_storage: Storage
    ):
        """
        Test file upload
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            # download should work
            await test_storage.get(remote_path, file_obj.name)

            # contents should match
            assert filecmp.cmp(local_path, file_obj.name)

    @pytest.mark.asyncio
    async def test_get_file_fail(self, test_storage: Storage):
        """
        Test file upload
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            # download should fail
            with pytest.raises(FileNotFoundError) as exc_info:
                await test_storage.get(Path("non/existent/path"), file_obj.name)
            assert str(exc_info.value) == "Remote file does not exist"

    @pytest.mark.asyncio
    async def test_stream_file_success(
        self, remote_path: Path, test_storage: Storage, text_file_content: str
    ):
        """
        Test file upload
        """
        file_stream = test_storage.get_file_stream(remote_path, chunk_size=5)
        read_bytes = b"".join([chunk async for chunk in file_stream])
        assert read_bytes.decode("utf-8") == text_file_content

    @pytest.mark.asyncio
    async def test_stream_file_fail(self, test_storage: Storage):
        """
        Test file upload
        """
        # get stream should fail
        with pytest.raises(FileNotFoundError) as exc_info:
            file_stream = test_storage.get_file_stream(Path("non/existent/path"), chunk_size=5)
            b"".join([chunk async for chunk in file_stream])

        assert str(exc_info.value) == "Remote file does not exist"

    @pytest.mark.asyncio
    async def test_get_text_success(self, test_storage: Storage, text_file_content):
        """
        Test file upload
        """
        # upload text
        remote_path = Path("some/text/file")
        await test_storage.put_text(text=text_file_content, remote_path=remote_path)

        # download should work
        value = await test_storage.get_text(remote_path)
        assert value == text_file_content

    @pytest.mark.asyncio
    async def test_get_object_success(self, test_storage: Storage, pydantic_object):
        """
        Dict object upload
        """
        # upload text
        remote_path = Path("some/json/file")
        await test_storage.put_object(data=pydantic_object, remote_path=remote_path)

        # download should work
        value = await test_storage.get_object(remote_path)
        assert value == pydantic_object.dict()

    @pytest.mark.asyncio
    async def test_delete_object_fail(self, test_storage: Storage):
        """
        Test file delete (missing)
        """
        with pytest.raises(FileNotFoundError) as exc_info:
            await test_storage.delete(Path("non/existent/path"))

        assert str(exc_info.value) == "Remote file does not exist"

    @pytest.mark.asyncio
    async def test_delete_object_success(self, test_storage: Storage, text_file_content):
        """
        Test file delete
        """
        # upload text
        remote_path = Path("some/delete/file")
        await test_storage.put_text(text=text_file_content, remote_path=remote_path)
        assert text_file_content == await test_storage.get_text(remote_path)

        # Delete text
        await test_storage.delete(remote_path)

        # Check missing
        with pytest.raises(FileNotFoundError) as exc_info:
            await test_storage.get_text(remote_path)

        assert str(exc_info.value) == "Remote file does not exist"
