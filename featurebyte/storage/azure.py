"""
Azure Storage Blob Class
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncGenerator

import aiofiles
from azure.core.exceptions import ResourceNotFoundError

from featurebyte.logging import get_logger
from featurebyte.storage.base import Storage

logger = get_logger(__name__)


class AzureBlobStorage(Storage):
    """
    Azure blob storage class
    """

    def __init__(self, get_client: Any, prefix: str = "featurebyte", temp: bool = False) -> None:
        """
        Initialize class

        Parameters
        ----------
        get_client: Any
            Context manager to create client
        prefix: str
            Prefix to use
        temp: bool
            Is temp data
        """
        self.get_client = get_client
        self.prefix = Path("temp").joinpath(prefix) if temp else Path(prefix)

    async def put(self, local_path: Path, remote_path: Path) -> None:
        """
        Upload local file to storage

        Parameters
        ----------
        local_path: Path
            Path to local file to be uploaded
        remote_path: Path
            Path of remote file to be stored

        Raises
        ------
        FileExistsError
            File already exists on remote path
        """

        async with self.get_client() as client:
            # check if blob exists
            remote_path = self.prefix.joinpath(remote_path)
            async with client.get_blob_client(str(remote_path)) as blob_client:
                try:
                    await blob_client.get_blob_properties()
                    raise FileExistsError("File already exists on remote path")
                except ResourceNotFoundError:
                    logger.debug(
                        "Put object to storage",
                        extra={
                            "container_name": client.container_name,
                            "object_name": str(remote_path),
                            "file_path": str(local_path),
                        },
                    )
                    with open(local_path, "rb") as file_obj:
                        await blob_client.upload_blob(file_obj)

    async def delete(self, remote_path: Path) -> None:
        """
        Delete file from storage

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be deleted

        Raises
        ------
        FileNotFoundError
            Remote file does not exist
        """
        async with self.get_client() as client:
            try:
                remote_path = self.prefix.joinpath(remote_path)
                await client.delete_blob(str(remote_path), delete_snapshots="include")
            except ResourceNotFoundError as exc:
                raise FileNotFoundError("Remote file does not exist") from exc

    async def _get(self, remote_path: Path, local_path: Path) -> None:
        """
        Download file from storage to local path

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        local_path: Path
            Path to stored downloaded file

        Raises
        ------
        FileNotFoundError
            Remote file does not exist
        """
        async with self.get_client() as client:
            remote_path = self.prefix.joinpath(remote_path)
            logger.debug(
                "Get object from storage",
                extra={
                    "container_name": client.container_name,
                    "object_name": str(remote_path),
                    "file_path": str(local_path),
                },
            )
            try:
                download_stream = await client.download_blob(blob=str(remote_path))
                async with aiofiles.open(local_path, "wb") as file_obj:
                    await file_obj.write(await download_stream.read())
            except ResourceNotFoundError as exc:
                raise FileNotFoundError("Remote file does not exist") from exc

    async def get_file_stream(
        self, remote_path: Path, chunk_size: int = 255 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream file from storage to local path

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        chunk_size: int
            Size of each chunk in the stream

        Yields
        ------
        bytes
            Byte chunk

        Raises
        ------
        FileNotFoundError
            Remote file does not exist
        """
        async with self.get_client() as client:
            remote_path = self.prefix.joinpath(remote_path)
            try:
                download_stream = await client.download_blob(blob=str(remote_path))
                while True:
                    data = await download_stream.read(chunk_size)
                    if not data:
                        break
                    yield data
            except ResourceNotFoundError as exc:
                raise FileNotFoundError("Remote file does not exist") from exc
