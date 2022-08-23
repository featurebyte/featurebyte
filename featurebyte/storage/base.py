"""
Storage base class
"""
from __future__ import annotations

from typing import AsyncGenerator

from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    """
    Base storage class
    """

    @abstractmethod
    async def put(self, local_path: Path, remote_path: str) -> None:
        """
        Upload local file to storage

        Parameters
        ----------
        local_path: Path
            Path to local file to be uploaded
        remote_path: str
            Path of remote file to be stored
        """

    @abstractmethod
    async def get(self, remote_path: str, local_path: Path) -> None:
        """
        Download file from storage to local path

        Parameters
        ----------
        remote_path: str
            Path of remote file to be downloaded
        local_path: Path
            Path to stored downloaded file
        """

    @abstractmethod
    async def get_file_stream(
        self, remote_path: str, chunk_size: int = 255 * 1024
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream file from storage to local path

        Parameters
        ----------
        remote_path: str
            Path of remote file to be downloaded
        chunk_size: int
            Size of each chunk in the stream
        """
        yield bytes()
