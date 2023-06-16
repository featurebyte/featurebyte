"""
Test SimpleStorage classes
"""
import json
import os
import tempfile

import pytest

from featurebyte import StorageType
from featurebyte.models.credential import (
    AzureBlobStorageCredential,
    GCSStorageCredential,
    KerberosKeytabCredential,
    S3StorageCredential,
)
from featurebyte.session.simple_storage import AzureBlobStorage, GCSStorage, S3SimpleStorage
from featurebyte.session.spark import SparkSession


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


@pytest.mark.skip(reason="Kerberos is not supported in CI")
def test_webhdfs_storage():
    """
    Test WebHDFS storage
    """
    # create a temporary krb5.conf file to specify the KDC
    with tempfile.NamedTemporaryFile(mode="w", suffix=".conf", delete=False) as krb5_config_file:
        kdc = "analytics-cluster-m.us-central1-c.c.vpc-host-nonprod-xa739-xz970.internal"
        krb5_config_file.write(
            f"[realms]\nDATAPROC.FEATUREBYTE.COM = {{\n  kdc = {kdc}\n  admin_server = {kdc}\n}}"
        )
        krb5_config_file.flush()
        os.environ["KRB5_CONFIG"] = krb5_config_file.name
        session = SparkSession(
            host="analytics-cluster-m.us-central1-c.c.vpc-host-nonprod-xa739-xz970.internal",
            port=10003,
            http_path="cliservice",
            use_http_transport=False,
            use_ssl=False,
            featurebyte_catalog="default",
            featurebyte_schema="featurebyte",
            storage_type=StorageType.WEBHDFS,
            storage_url="https://analytics-cluster-m.us-central1-c.c.vpc-host-nonprod-xa739-xz970.internal:9871/tmp/storage-test",
            storage_spark_url="hdfs://tmp/storage-test",
            database_credential=KerberosKeytabCredential.from_file(
                keytab_filepath="tests/fixtures/hive.service.keytab",
                principal="hive/analytics-cluster-m.us-central1-c.c.vpc-host-nonprod-xa739-xz970.internal@DATAPROC.FEATUREBYTE.COM",
            ),
        )
        session.test_storage_connection()
