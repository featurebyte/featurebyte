"""
Storage base class
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, AsyncGenerator, Optional

import aiofiles
import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel


class StorageCache:
    """Storage cache class"""

    _cache_lock: Any = asyncio.Lock()

    def __init__(self) -> None:
        self._cache_dir = tempfile.mkdtemp()

    def __del__(self) -> None:
        if os.path.exists(self._cache_dir):
            shutil.rmtree(self._cache_dir)

    def clear(self) -> None:
        """
        Clear cache
        """
        shutil.rmtree(self._cache_dir)
        os.makedirs(self._cache_dir)

    def exists(self, cache_key: str) -> bool:
        """
        Check if cache key exists

        Parameters
        ----------
        cache_key: str
            Cache key

        Returns
        -------
        bool
            True if cache key exists, False otherwise
        """
        cache_path = Path(self._cache_dir) / cache_key
        return cache_path.exists()

    async def write_to_cache(self, cache_key: str, local_path: Path) -> None:
        """
        Write file to cache

        Parameters
        ----------
        cache_key: str
            Cache key
        local_path: Path
            Local path to store file
        """
        cache_path = Path(self._cache_dir) / cache_key
        async with self._cache_lock:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local_path, cache_path)

    async def write_from_cache(self, cache_key: str, local_path: Path) -> bool:
        """
        Read file from cache

        Parameters
        ----------
        cache_key: str
            Cache key
        local_path: Path
            Local path to store file

        Returns
        -------
        bool
            True if cache hit, False otherwise
        """
        cache_path = Path(self._cache_dir) / cache_key
        if cache_path.exists():
            shutil.copy(cache_path, local_path)
            return True
        return False


class Storage(ABC):
    """
    Base storage class
    """

    _cache: Any = StorageCache()

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

    async def try_delete_if_exists(self, remote_path: Path) -> None:
        """
        Try to delete file in storage

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be deleted
        """
        try:
            await self.delete(remote_path)
        except FileNotFoundError:
            pass

    async def get(
        self, remote_path: Path, local_path: Path, cache_key: Optional[str] = None
    ) -> None:
        """
        Download file from storage to local path with caching if cache_key is provided

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        local_path: Path
            Path to stored downloaded file
        cache_key: Optional[str]
            Cache key for storing downloaded file (if provided, the result will be cached).

        Returns
        -------
        None
        """
        if cache_key is None:
            return await self._get(remote_path, local_path)

        cache_hit = await self._cache.write_from_cache(cache_key, local_path)
        if not cache_hit:
            await self._get(remote_path, local_path)
            await self._cache.write_to_cache(cache_key, local_path)

    @abstractmethod
    async def _get(self, remote_path: Path, local_path: Path) -> None:
        """
        Retrieve file from storage to local path

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        local_path: Path
            Path to stored downloaded file
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

    async def put_object(self, data: BaseModel, remote_path: Path) -> None:
        """
        Upload pydantic object to storage as json file

        Parameters
        ----------
        data: BaseModel
            Pydantic object that can be serialized to json
        remote_path: Path
            Path of remote file to upload to
        """
        await self.put_text(data.model_dump_json(), remote_path)

    async def get_object(self, remote_path: Path) -> Any:
        """
        Download pydantic object from stored json file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded

        Returns
        -------
        Any
            Python object
        """
        return json.loads(await self.get_text(remote_path))

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
        async with aiofiles.tempfile.NamedTemporaryFile(mode="w") as file_obj:
            await file_obj.write(text)
            await file_obj.flush()
            await self.put(Path(str(file_obj.name)), remote_path)

    async def get_text(self, remote_path: Path, cache_key: Optional[str] = None) -> str:
        """
        Download text content from storage text file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        cache_key: Optional[str]
            Cache key for storing downloaded file (if provided, the result will be cached)

        Returns
        -------
        str
            Text data
        """
        async with aiofiles.tempfile.NamedTemporaryFile(mode="r") as tmp_file:
            await self.get(remote_path, Path(str(tmp_file.name)), cache_key=cache_key)
            async with aiofiles.open(tmp_file.name, encoding="utf8") as file_obj:
                text = await file_obj.read()
            return text

    async def put_bytes(self, content: bytes, remote_path: Path) -> None:
        """
        Upload bytes content to storage as bytes file

        Parameters
        ----------
        content: bytes
            Bytes value to be stored
        remote_path: Path
            Path of remote file to upload to
        """
        async with aiofiles.tempfile.NamedTemporaryFile(mode="w+b") as file_obj:
            await file_obj.write(content)
            await file_obj.flush()
            await self.put(Path(str(file_obj.name)), remote_path)

    async def get_bytes(self, remote_path: Path, cache_key: Any = None) -> bytes:
        """
        Download bytes content from storage bytes file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        cache_key: Any
            Cache key for storing downloaded file (if provided, the result will be cached)

        Returns
        -------
        bytes
            Bytes data
        """
        async with aiofiles.tempfile.NamedTemporaryFile(mode="r+b") as tmp_file:
            await self.get(remote_path, Path(str(tmp_file.name)), cache_key=cache_key)
            async with aiofiles.open(tmp_file.name, mode="r+b") as file_obj:
                content = await file_obj.read()
            return content

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
        async with aiofiles.tempfile.NamedTemporaryFile() as file_obj:
            dataframe.to_parquet(file_obj.name)
            await self.put(Path(str(file_obj.name)), remote_path)

    async def get_dataframe(self, remote_path: Path, cache_key: Any = None) -> DataFrame:
        """
        Download dataframe from storage parquet file

        Parameters
        ----------
        remote_path: Path
            Path of remote file to be downloaded
        cache_key: Any
            Cache key for storing downloaded file (if provided, the result will be cached)

        Returns
        -------
        DataFrame
            Pandas DataFrame object
        """
        async with aiofiles.tempfile.NamedTemporaryFile(mode="r") as file_obj:
            await self.get(remote_path, Path(str(file_obj.name)), cache_key=cache_key)
            return pd.read_parquet(file_obj.name)
