"""
SimpleStorage classes for basic object store operations
"""

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import boto3
from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import BlobServiceClient
from bson import ObjectId
from google.cloud.storage import Client as GCSClient
from smart_open import open as remote_open

from featurebyte.models.credential import (
    AzureBlobStorageCredential,
    GCSStorageCredential,
    S3StorageCredential,
    StorageCredential,
)
from featurebyte.session.webhdfs import webhdfs_delete, webhdfs_open

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

    def test_connection(self) -> None:
        """
        Test connection to storage
        """
        conn_test_filename = f"_conn_test_{ObjectId()}"
        with self.open(path=conn_test_filename, mode="wb") as file_obj:
            file_obj.write(b"OK")
        self.delete_object(path=conn_test_filename)

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
        storage_credential: Optional[S3StorageCredential] = None,
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

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        key = f"{self.key_prefix}/{path}" if self.key_prefix else path
        self.client.delete_object(Bucket=self.bucket, Key=key)


class GCSStorage(SimpleStorage):
    """
    Simple GCS storage class
    """

    def __init__(
        self,
        storage_url: str,
        storage_credential: GCSStorageCredential,
    ) -> None:
        super().__init__(storage_url=storage_url, storage_credential=storage_credential)

        self.client = GCSClient.from_service_account_info(
            info=storage_credential.service_account_info
        )
        protocol, path = storage_url.split("//")
        assert protocol == "gs:", "GCSStorage: Protocol must be gs for storage_url"
        parts = path.split("/")
        if len(parts) == 0:
            raise ValueError("Bucket is missing in storage url")
        self.bucket = parts[0]
        if len(parts) > 1:
            self.key_prefix = "/".join(parts[1:])
            self.base_url = f"gs://{self.bucket}/{self.key_prefix}"
        else:
            self.key_prefix = ""
            self.base_url = f"gs://{self.bucket}"

    def _get_transport_params(self) -> Dict[str, Any]:
        return {"client": self.client}

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        key = f"{self.key_prefix}/{path}" if self.key_prefix else path
        self.client.get_bucket(self.bucket).delete_blob(blob_name=key)


class AzureBlobStorage(SimpleStorage):
    """
    Simple Azure Blob storage class
    """

    def __init__(
        self,
        storage_url: str,
        storage_credential: AzureBlobStorageCredential,
    ) -> None:
        super().__init__(storage_url=storage_url, storage_credential=storage_credential)
        self.client = BlobServiceClient.from_connection_string(
            conn_str=(
                f"AccountName={storage_credential.account_name};DefaultEndpointsProtocol=https;"
                "EndpointSuffix=core.windows.net"
            ),
            credential=AzureNamedKeyCredential(
                name=storage_credential.account_name, key=storage_credential.account_key
            ),
        )
        protocol, path = storage_url.split("//")
        assert protocol == "azure:", "AzureBlobStorage: Protocol must be azure for storage_url"
        parts = path.split("/")
        if len(parts) == 0:
            raise ValueError("Container is missing in storage url")
        self.container = parts[0]
        if len(parts) > 1:
            self.key_prefix = "/".join(parts[1:])
            self.base_url = f"azure://{self.container}/{self.key_prefix}"
        else:
            self.key_prefix = ""
            self.base_url = f"azure://{self.container}"

    def _get_transport_params(self) -> Dict[str, Any]:
        return {"client": self.client}

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        key = f"{self.key_prefix}/{path}" if self.key_prefix else path
        self.client.get_container_client(container=self.container).delete_blob(blob=key)


class WebHDFSStorage(SimpleStorage):
    """
    Simple WebHDFS storage class
    """

    def __init__(
        self,
        storage_url: str,
        kerberos: bool = False,
    ) -> None:
        self.kerberos = kerberos
        super().__init__(storage_url=storage_url)
        protocol, path = storage_url.split("//")
        assert protocol in {
            "http:",
            "https:",
        }, "WebHDFS: protocol must be http or https for storage_url"
        self.ssl = protocol == "https:"
        parts = path.split("/")
        if len(parts) == 0:
            raise ValueError("HDFS hostname is missing in storage url")
        self.hostname = parts[0]
        if len(parts) > 1:
            self.key_prefix = "/".join(parts[1:])
            self.base_url = f"webhdfs://{self.hostname}/{self.key_prefix}"
        else:
            self.key_prefix = ""
            self.base_url = f"webhdfs://{self.hostname}"

    @contextmanager
    def open(self, path: str, mode: FileMode) -> Any:
        path = path.lstrip("/")
        with webhdfs_open(
            f"{self.base_url}/{path}",
            mode=mode,
            kerberos=self.kerberos,
            ssl=self.ssl,
        ) as file_obj:
            yield file_obj

    def delete_object(self, path: str) -> None:
        path = path.rstrip("/")
        webhdfs_delete(f"{self.base_url}/{path}", kerberos=self.kerberos, ssl=self.ssl)
