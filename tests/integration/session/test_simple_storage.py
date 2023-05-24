"""
Test SimpleStorage classes
"""
import json
import os

from featurebyte.models.credential import (
    AzureBlobStorageCredential,
    GCSStorageCredential,
    S3StorageCredential,
)
from featurebyte.session.simple_storage import AzureBlobStorage, GCSStorage, S3SimpleStorage


def test_gcs_storage():
    """
    Test GCSStorage
    """
    info = json.loads(os.environ["GCS_CLOUD_STORAGE_RW_TEST"])
    storage = GCSStorage(
        storage_url="gs://featurebyte-test-bucket",
        storage_credential=GCSStorageCredential(service_account_info=info),
    )
    storage.test_connection()


def test_s3_storage():
    """
    Test S3 storage
    """
    storage = S3SimpleStorage(
        storage_url=f'{os.getenv("DATABRICKS_STORAGE_URL")}/test',
        storage_credential=S3StorageCredential(
            s3_access_key_id=os.environ["DATABRICKS_STORAGE_ACCESS_KEY_ID"],
            s3_secret_access_key=os.environ["DATABRICKS_STORAGE_ACCESS_KEY_SECRET"],
        ),
    )
    storage.test_connection()


def test_azure_blob_storage():
    """
    Test Azure Blob storage
    """
    storage = AzureBlobStorage(
        storage_url="azure://storage-test",
        storage_credential=AzureBlobStorageCredential(
            account_name=os.environ["AZURE_STORAGE_ACCOUNT_NAME"],
            account_key=os.environ["AZURE_STORAGE_ACCOUNT_KEY"],
        ),
    )
    storage.test_connection()
