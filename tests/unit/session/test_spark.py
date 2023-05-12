"""
Test SparkSession
"""
from unittest.mock import patch

import pytest

from featurebyte import S3StorageCredential, StorageType
from featurebyte.models.credential import GCSStorageCredential
from featurebyte.session.spark import SparkSession


@patch("featurebyte.session.spark.HiveConnection.__new__")
def test_s3_storage(config):
    """
    Test initializing session with s3 storage
    """
    params = dict(
        host="localhost",
        port=10000,
        use_http_transport=False,
        use_ssl=False,
        http_path="cliservice",
        storage_type=StorageType.S3,
        storage_url="https://storage.googleapis.com/test/",
        storage_spark_url="s3://test/",
        featurebyte_catalog="spark_catalog",
        featurebyte_schema="featurebyte",
    )

    # S3 Storage requires a credential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(**params)
    assert "Storage credential is required for S3" in str(exc)

    # S3 Storage requires a S3StorageCredential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(storage_credential=GCSStorageCredential(service_account_info={}), **params)
    assert "Unsupported storage credential for S3: GCSStorageCredential" in str(exc)

    # Success
    # with pytest.raises(ValueError):
    SparkSession(
        storage_credential=S3StorageCredential(
            s3_access_key_id="test", s3_secret_access_key="test"
        ),
        **params,
    )


@patch("featurebyte.session.spark.HiveConnection.__new__")
def test_gcs_storage(config):
    """
    Test initializing session with gcs storage
    """
    params = dict(
        host="localhost",
        port=10000,
        use_http_transport=False,
        use_ssl=False,
        http_path="cliservice",
        storage_type=StorageType.GCS,
        storage_url="gs://test/",
        storage_spark_url="gs://test/",
        featurebyte_catalog="spark_catalog",
        featurebyte_schema="featurebyte",
    )

    # GCS Storage requires a credential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(**params)
    assert "Storage credential is required for GCS" in str(exc)

    # GCS Storage requires a GCSStorageCredential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(
            storage_credential=S3StorageCredential(
                s3_access_key_id="test", s3_secret_access_key="test"
            ),
            **params,
        )
    assert "Unsupported storage credential for GCS: S3StorageCredential" in str(exc)

    # Success
    with patch("featurebyte.session.simple_storage.GCSClient.from_service_account_info"):
        SparkSession(
            storage_credential=GCSStorageCredential(
                service_account_info={
                    "client_email": "test",
                    "token_uri": "test",
                    "private_key": "test",
                }
            ),
            **params,
        )
