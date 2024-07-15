"""
Webdav storage class (rclone)
"""

import re
from http import HTTPStatus
from pathlib import Path
from typing import AsyncGenerator

import httpx

from featurebyte.enum import StrEnum
from featurebyte.logging import get_logger
from featurebyte.storage.base import Storage

logger = get_logger(__name__)


class WebdavHTTPMethods(StrEnum):
    """
    Webdav HTTP methods
    """

    PROPFIND = "PROPFIND"
    MKCOL = "MKCOL"
    COPY = "COPY"
    MOVE = "MOVE"
    LOCK = "LOCK"
    UNLOCK = "UNLOCK"


class WebdavStorage(Storage):
    """
    Webdav storage class
    """

    def __init__(self, base_url: str, temp: bool = False) -> None:
        """
        Initialize class

        Parameters
        ----------
        base_url: str
            Base URL of storage
        temp: bool
            Is temp data
        """
        base_url = base_url.rstrip("/")

        self.client = httpx.AsyncClient()
        self.base_url = base_url  # http://localhost:1234
        self.temp = temp

    async def __adel__(self) -> None:
        await self.client.aclose()

    async def mkdir(self, remote_path: Path) -> None:
        """
        Create directory in storage, similar to mkdir -p

        Parameters
        ----------
        remote_path: Path
            Path of remote directories to be created

        Raises
        ------
        FileExistsError
            File already exists on remote path
        """
        paths = str(remote_path).strip("/").split("/")
        mkdir_path = ""
        for path in paths[:-1]:
            mkdir_path = f"{mkdir_path}/{path}"
            request = self.client.build_request(
                WebdavHTTPMethods.PROPFIND, url=f"{self.base_url}{mkdir_path}"
            )
            response = await self.client.send(request)
            if response.status_code == HTTPStatus.NOT_FOUND:
                request = self.client.build_request(
                    WebdavHTTPMethods.MKCOL, url=f"{self.base_url}{mkdir_path}"
                )
                response = await self.client.send(request)
                assert response.status_code == HTTPStatus.CREATED
            elif response.status_code == HTTPStatus.MULTI_STATUS:
                pat = re.compile(r"<D:href>(.*?)</D:href>")
                mat = pat.search(response.text)

                if mat:
                    # if the last character is a slash, it is a directory
                    if mat.group(1).endswith("/"):
                        continue
                    # if the last character is not a slash, it is a file
                    raise FileExistsError("Remote path cannot be created")
                raise FileExistsError(
                    "rclone did not return a correct response whilst creating directory"
                )
            else:
                raise FileExistsError("Unknown error occurred while creating directory")

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
            File or path already exists on remote path
        """
        if self.temp:
            remote_path = Path("temp").joinpath(remote_path)

        # Create all subdirectories
        await self.mkdir(remote_path)

        request = self.client.build_request(
            WebdavHTTPMethods.PROPFIND, url=f"{self.base_url}/{remote_path}"
        )
        response = await self.client.send(request)
        if response.status_code != HTTPStatus.NOT_FOUND:
            raise FileExistsError("File already exists on remote path")

        with open(local_path, "rb") as file_obj:
            logger.debug(
                "Put object to storage",
                extra={
                    "object_name": str(remote_path),
                    "file_path": str(local_path),
                },
            )
            response = await self.client.put(
                url=f"{self.base_url}/{remote_path}", content=file_obj.read()
            )

    async def delete(self, remote_path: Path) -> None:
        """
        Delete file in storage

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be deleted

        Raises
        ------
        FileNotFoundError
            Remote file does not exist
        """
        if self.temp:
            remote_path = Path("temp").joinpath(remote_path)

        response = await self.client.delete(url=f"{self.base_url}/{remote_path}")

        if response.status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError("Remote file does not exist")
        assert response.status_code == HTTPStatus.NO_CONTENT

    async def _get(self, remote_path: Path, local_path: Path) -> None:
        """
        Retrieve file from storage to local path

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
        if self.temp:
            remote_path = Path("temp").joinpath(remote_path)

        response = await self.client.get(url=f"{self.base_url}/{remote_path}")
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError("Remote file does not exist")
        with open(local_path, "wb") as file_obj:
            file_obj.write(response.content)

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
        if self.temp:
            remote_path = Path("temp").joinpath(remote_path)

        async with self.client.stream("GET", f"{self.base_url}/{remote_path}") as response:
            if response.status_code == HTTPStatus.NOT_FOUND:
                raise FileNotFoundError("Remote file does not exist")
            async for chunk in response.aiter_bytes():
                yield chunk
