"""
SimpleStorage classes for basic object store operations
"""
from typing import Any, Literal, Optional

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path

import boto3
from smart_open import open as remote_open

from featurebyte.models.credential import S3Credential, StorageCredentialType

FileMode = Literal["r", "w", "rb", "wb"]


class SimpleStorage(ABC):
    """
    Base class for simple storage manager class
    """

    def __init__(
        self, storage_url: str, storage_credential: Optional[StorageCredentialType] = None
    ) -> None:
        """
        Initialize storage class

        Parameters
        ----------
        storage_url: str
            Url for storage
        storage_credential: Optional[StorageCredentialType]
            Credential for storage access
        """
        self.client: Any = None
        self.storage_url = storage_url
        self.storage_credential = storage_credential
        self.base_url: str = ""

    def test_connection(self) -> None:
        """
        Test connection to storage
        """
        with self.open(path="_conn_test", mode="w") as file_obj:
            file_obj.write("OK")
        self.delete_object(path="_conn_test")

    @abstractmethod
    def delete_object(self, path: str) -> None:
        """
        Delete object from storage
        """

    @contextmanager
    def open(self, path: str, mode: FileMode) -> Any:
        """
        Open file object at path for read / write operations

        Parameters
        ----------
        path: str
            Path of object
        mode: FileMode
            IO mode

        Yields
        -------
        A file-like object.
        """
        path = path.lstrip("/")
        yield remote_open(
            f"{self.base_url}/{path}",
            mode=mode,
            transport_params={"client": self.client} if self.client else {},
        )


class FileSimpleStorage(SimpleStorage):
    """
    Simple file storage class
    """

    def __init__(self, storage_url: str) -> None:
        super().__init__(storage_url=storage_url, storage_credential=None)
        path = Path(storage_url).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        self.base_url = f"file://{str(path)}"

    def delete_object(self, path: str) -> None:
        base_path = self.base_url.replace("file://", "")
        os.remove(f"{base_path}/{path}")


class S3SimpleStorage(SimpleStorage):
    """
    Simple S3 storage class
    """

    def __init__(
        self,
        storage_url: str,
        storage_credential: Optional[StorageCredentialType] = None,
        region_name: Optional[str] = None,
    ) -> None:
        super().__init__(storage_url=storage_url, storage_credential=storage_credential)
        session_params = {"region_name": region_name}
        if isinstance(storage_credential, S3Credential):
            session_params["aws_access_key_id"] = storage_credential.s3_access_key_id
            session_params["aws_secret_access_key"] = storage_credential.s3_secret_access_key
        else:
            raise NotImplementedError("Unsupported remote storage credential")
        self.session = boto3.Session(**session_params)
        self.client = self.session.client("s3", endpoint_url=storage_url)
        self.base_url = "s3://"
        parts = storage_url.split("//")[1:]
        self.bucket = parts[0]
        self.key_prefix = "/".join(parts[1:])

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        self.client.delete_object(Bucket=self.bucket, Key=f"{self.key_prefix}/{path}")
