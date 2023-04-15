"""
SimpleStorage classes for basic object store operations
"""
from typing import Any, Dict, Literal, Optional

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path

import boto3
from bson import ObjectId
from smart_open import open as remote_open

from featurebyte.models.credential import S3StorageCredential, StorageCredential

FileMode = Literal["r", "w", "rb", "wb"]


class SimpleStorage(ABC):
    """
    Base class for simple storage manager class
    """

    def __init__(
        self, storage_url: str, storage_credential: Optional[StorageCredential] = None
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
        self.storage_url = storage_url
        self.storage_credential = storage_credential
        self.base_url: str = ""

    def _get_transport_params(self) -> Dict[str, Any]:
        """
        Get transport parameters

        Returns
        -------
        Dict[str, Any]
        """
        return {}

    @abstractmethod
    def test_connection(self) -> None:
        """
        Test connection to storage
        """

    @abstractmethod
    def delete_object(self, path: str) -> None:
        """
        Delete object from storage

        Parameters
        ----------
        path: str
            Path of object to delete
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
        Any
            A file-like object
        """
        path = path.lstrip("/")
        with remote_open(
            f"{self.base_url}/{path}",
            mode=mode,
            transport_params=self._get_transport_params(),
        ) as file_obj:
            yield file_obj


class FileSimpleStorage(SimpleStorage):
    """
    Simple file storage class
    """

    def __init__(self, storage_url: str) -> None:
        super().__init__(storage_url=storage_url, storage_credential=None)
        path = Path(storage_url).expanduser()
        path.mkdir(parents=True, exist_ok=True)
        self.base_url = f"file://{str(path)}"

    def test_connection(self) -> None:
        conn_test_filename = f"_conn_test_{ObjectId()}"
        with self.open(path=conn_test_filename, mode="w") as file_obj:
            file_obj.write("OK")
        self.delete_object(path=conn_test_filename)

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
        storage_credential: Optional[StorageCredential] = None,
        region_name: Optional[str] = None,
    ) -> None:
        super().__init__(storage_url=storage_url, storage_credential=storage_credential)
        session_params = {"region_name": region_name}
        if isinstance(storage_credential, S3StorageCredential):
            session_params["aws_access_key_id"] = storage_credential.s3_access_key_id
            session_params["aws_secret_access_key"] = storage_credential.s3_secret_access_key
        else:
            raise NotImplementedError(
                f"Unsupported remote storage credential: {storage_credential}"
            )

        protocol, path = storage_url.split("//")
        parts = path.split("/")
        if len(parts) == 1:
            raise ValueError("Bucket is missing in storage url")
        endpoint_url = f"{protocol}//{parts[0]}"
        self.bucket = parts[1]
        if len(parts) > 2:
            self.key_prefix = "/".join(parts[2:])
            self.base_url = f"s3://{self.bucket}/{self.key_prefix}"
        else:
            self.key_prefix = ""
            self.base_url = f"s3://{self.bucket}"
        self.session = boto3.Session(**session_params)
        self.client = self.session.client("s3", endpoint_url=endpoint_url)

    def _get_transport_params(self) -> Dict[str, Any]:
        return {"client": self.client}

    def test_connection(self) -> None:
        # should raise exception if connection is not valid
        self.client.list_objects(Bucket=self.bucket)

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        key = f"{self.key_prefix}/{path}" if self.key_prefix else path
        self.client.delete_object(Bucket=self.bucket, Key=key)
