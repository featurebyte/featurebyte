"""
Webdav storage class (rclone)
"""

from typing import AsyncGenerator

from http import HTTPStatus
from pathlib import Path

import httpx

from featurebyte.enum import StrEnum
from featurebyte.storage.base import Storage


class WebdavHTTPMethods(StrEnum):
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

    def __init__(self, base_url: str) -> None:
        """
        Initialize class
        """
        base_url = base_url.rstrip("/")

        self.client = httpx.AsyncClient()
        self.base_url = base_url  # http://localhost:1234

    async def __adel__(self):
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
        mkdir_path = "/"
        for path in paths[:-1]:
            mkdir_path = f"{mkdir_path}/{path}"
            request = self.client.build_request(
                WebdavHTTPMethods.PROPFIND, url=f"{self.base_url}/{mkdir_path}"
            )
            response = await self.client.send(request)
            if response.status_code == HTTPStatus.NOT_FOUND:
                request = self.client.build_request(
                    WebdavHTTPMethods.MKCOL, url=f"{self.base_url}/{mkdir_path}"
                )
                await self.client.send(request)
            elif response.status_code != HTTPStatus.MULTI_STATUS:
                raise FileExistsError("Remote path cannot be created")

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

        # Create all subdirectories
        await self.mkdir(remote_path)

        request = self.client.build_request(
            WebdavHTTPMethods.PROPFIND, url=f"{self.base_url}/{remote_path}"
        )
        response = await self.client.send(request)
        if response.status_code != HTTPStatus.NOT_FOUND:
            raise FileExistsError("File already exists on remote path")

        with open(local_path, "rb") as file_obj:
            await self.client.put(url=f"{self.base_url}/{remote_path}", content=file_obj.read())

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
        """
        response = await self.client.delete(url=f"{self.base_url}/{remote_path}")
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError("Remote file does not exist")
        elif response.status_code == HTTPStatus.NO_CONTENT:
            return
        raise FileNotFoundError("")

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
        """
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
        """
        response = await self.client.get(url=f"{self.base_url}/{remote_path}")
        if response.status_code == HTTPStatus.NOT_FOUND:
            raise FileNotFoundError("Remote file does not exist")
        for chunk in response.iter_bytes(chunk_size):
            yield chunk
