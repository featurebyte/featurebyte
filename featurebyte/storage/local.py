"""
Local storage class
"""
from typing import AsyncGenerator

import shutil
from pathlib import Path

from featurebyte.storage.base import Storage


class LocalStorage(Storage):
    """
    Local storage class
    """

    def __init__(self, base_path: Path) -> None:
        """
        Initialize local storage location
        """
        base_path = Path(base_path)
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        self._base_path = base_path

    @property
    def base_path(self) -> Path:
        """
        Base path

        Returns
        -------
        Path:
            Base path
        """
        return self._base_path

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
        destination_path = self._base_path.joinpath(remote_path)
        if destination_path.exists():
            raise FileExistsError("File already exists on remote path")

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(local_path, destination_path)

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
        source_path = self._base_path.joinpath(remote_path)
        if not source_path.exists():
            raise FileNotFoundError("Remote file does not exist")

        shutil.copy(source_path, local_path)

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
        source_path = self._base_path.joinpath(remote_path)
        if not source_path.exists():
            raise FileNotFoundError("Remote file does not exist")

        with open(source_path, "rb") as file_obj:
            while True:
                chunk = file_obj.read(chunk_size)
                if len(chunk) == 0:
                    break
                yield chunk
