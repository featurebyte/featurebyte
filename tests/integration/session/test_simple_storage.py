"""
Test SimpleStorage classes
"""
import json
import os

from featurebyte.models.credential import GCSStorageCredential, S3StorageCredential
from featurebyte.session.simple_storage import GCSStorage, S3SimpleStorage


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
