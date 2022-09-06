"""
Storage base class
"""
from __future__ import annotations

from typing import Any, AsyncGenerator

import pickle
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
from pandas import DataFrame


class Storage(ABC):
    """
    Base storage class
    """

    @abstractmethod
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

    @abstractmethod
    async def get(self, remote_path: Path, local_path: Path) -> None:
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

    @abstractmethod
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
        """
        yield bytes()

    async def put_object(self, data: Any, remote_path: Path) -> None:
        """
        Upload python object to storage as pickle file

        Parameters
        ----------
        data: Any
            Python object that can be pickled
        remote_path: Path
            Path of remote file to upload to
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            pickle.dump(data, file_obj)
            file_obj.flush()
            await self.put(Path(file_obj.name), remote_path)

    async def get_object(self, remote_path: Path) -> Any:
        """
        Download python object from stored pickle file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded

        Returns
        -------
        Any
            Python object
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            await self.get(remote_path, Path(file_obj.name))
            return pickle.load(file_obj)

    async def put_text(self, text: str, remote_path: Path) -> None:
        """
        Upload text content to storage as text file

        Parameters
        ----------
        text: str
            Text value to be stored
        remote_path: Path
            Path of remote file to upload to
        """
        with tempfile.NamedTemporaryFile(mode="w") as file_obj:
            file_obj.write(text)
            file_obj.flush()
            await self.put(Path(file_obj.name), remote_path)

    async def get_text(self, remote_path: Path) -> str:
        """
        Download text content from storage text file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded

        Returns
        -------
        str
            Text data
        """
        with tempfile.NamedTemporaryFile(mode="r") as file_obj:
            await self.get(remote_path, Path(file_obj.name))
            return file_obj.read()

    async def put_dataframe(self, dataframe: DataFrame, remote_path: Path) -> None:
        """
        Upload dataframe to storage as parquet file

        Parameters
        ----------
        dataframe: DataFrame
            Pandas DataFrame to be stored
        remote_path: Path
            Path of remote file to upload to
        """
        with tempfile.NamedTemporaryFile() as file_obj:
            dataframe.to_parquet(file_obj.name)
            await self.put(Path(file_obj.name), remote_path)

    async def get_dataframe(self, remote_path: Path) -> DataFrame:
        """
        Download dataframe from storage parquet file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded

        Returns
        -------
        DataFrame
            Pandas DataFrame object
        """
        with tempfile.NamedTemporaryFile(mode="r") as file_obj:
            await self.get(remote_path, Path(file_obj.name))
            return pd.read_parquet(file_obj.name)
