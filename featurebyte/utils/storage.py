"""
Utility functions for file storage
"""
from __future__ import annotations

from typing import AsyncIterator

import os
from contextlib import asynccontextmanager

from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session

from featurebyte.config import Configurations
from featurebyte.storage import LocalStorage, LocalTempStorage, S3Storage, Storage

STORAGE_TYPE = os.environ.get("STORAGE_TYPE", "local")

S3_URL = os.environ.get("S3_URL")
S3_REGION_NAME = os.environ.get("S3_REGION_NAME")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "featurebyte")


@asynccontextmanager
async def get_client() -> AsyncIterator[AioBaseClient]:
    """
    Get an s3 client generated from settings.MinioSettings

    Yields
    ------
    AsyncIterator[AioBaseClient]
        s3 client
    """
    session = get_session()
    async with session.create_client(
        service_name="s3",
        region_name=S3_REGION_NAME,
        endpoint_url=S3_URL,
        use_ssl=S3_URL.startswith("https://") if S3_URL else True,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    ) as client:
        yield client


def get_storage() -> Storage:
    """
    Return global Storage object

    Returns
    -------
    Storage
        Storage object

    Raises
    ------
    ValueError
        Invalid storage type
    """
    if STORAGE_TYPE == "local":
        return LocalStorage(base_path=Configurations().storage.local_path)
    if STORAGE_TYPE == "s3":
        return S3Storage(get_client=get_client, bucket_name=S3_BUCKET_NAME)
    raise ValueError(f"Invalid storage type: {STORAGE_TYPE}")


def get_temp_storage() -> Storage:
    """
    Return temp storage

    Returns
    -------
    Storage
        Storage object

    Raises
    ------
    ValueError
        Invalid storage type
    """
    if STORAGE_TYPE == "local":
        return LocalTempStorage()
    if STORAGE_TYPE == "s3":
        return S3Storage(get_client=get_client, bucket_name=S3_BUCKET_NAME, temp=True)
    raise ValueError(f"Invalid storage type: {STORAGE_TYPE}")
