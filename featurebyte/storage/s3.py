"""
S3 Storage Class
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, AsyncGenerator

import aiofiles
from botocore.exceptions import ClientError

from featurebyte.logging import get_logger
from featurebyte.storage.base import Storage

logger = get_logger(__name__)


class S3Storage(Storage):
    """
    S3 storage class
    """

    def __init__(
        self, get_client: Any, bucket_name: str, prefix: str = "featurebyte", temp: bool = False
    ) -> None:
        """
        Initialize class

        Parameters
        ----------
        get_client: Any
            Context manager to create client
        bucket_name: str
            Bucket to use
        prefix: str
            Prefix to use
        temp: bool
            Is temp data
        """
        self.get_client = get_client
        self.bucket_name = bucket_name
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
            try:
                await client.head_bucket(Bucket=self.bucket_name)
            except ClientError:
                # create bucket if it does not exist
                await client.create_bucket(Bucket=self.bucket_name)

            remote_path = self.prefix.joinpath(remote_path)

            # check if key exists
            try:
                await client.head_object(Bucket=self.bucket_name, Key=str(remote_path))
                raise FileExistsError("File already exists on remote path")
            except ClientError:
                pass

            logger.debug(
                "Put object to storage",
                extra={
                    "bucket_name": self.bucket_name,
                    "object_name": str(remote_path),
                    "file_path": str(local_path),
                },
            )
            with open(local_path, "rb") as file_obj:
                await client.upload_fileobj(
                    Bucket=self.bucket_name, Key=str(remote_path), Fileobj=file_obj
                )

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
            remote_path = self.prefix.joinpath(remote_path)

            # check if key exists
            try:
                await client.head_object(Bucket=self.bucket_name, Key=str(remote_path))
            except ClientError as exc:
                raise FileNotFoundError("Remote file does not exist") from exc

            await client.delete_object(Bucket=self.bucket_name, Key=str(remote_path))

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
                    "bucket_name": self.bucket_name,
                    "object_name": str(remote_path),
                    "file_path": str(local_path),
                },
            )
            try:
                response = await client.get_object(Bucket=self.bucket_name, Key=str(remote_path))
                async with aiofiles.open(local_path, "wb") as file_obj:
                    async with response["Body"] as stream:
                        await file_obj.write(await stream.read())
            except client.exceptions.NoSuchKey as exc:
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
                response = await client.get_object(
                    Bucket=self.bucket_name,
                    Key=str(remote_path),
                )
                body = response["Body"]

                while True:
                    data = await body.read(chunk_size)
                    if not data:
                        break
                    yield data
            except client.exceptions.NoSuchKey as exc:
                raise FileNotFoundError("Remote file does not exist") from exc
