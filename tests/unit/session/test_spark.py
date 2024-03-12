"""
Test SparkSession
"""
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte import S3StorageCredential, StorageType
from featurebyte.models.credential import GCSStorageCredential
from featurebyte.query_graph.model.table import TableDetails
from featurebyte.session.spark import SparkSession


@pytest.fixture(name="spark_session_params")
def fixture_spark_session_params():
    """
    Spark session params
    """
    return dict(
        host="localhost",
        port=10000,
        use_http_transport=False,
        use_ssl=False,
        http_path="cliservice",
        storage_type=StorageType.S3,
        storage_url="https://storage.googleapis.com/test/",
        storage_path="s3://test/",
        catalog_name="spark_catalog",
        schema_name="featurebyte",
    )


@patch("featurebyte.session.spark.HiveConnection.__new__")
def test_s3_storage(config, spark_session_params):
    """
    Test initializing session with s3 storage
    """
    # S3 Storage requires a credential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(**spark_session_params)
    assert "Storage credential is required for S3" in str(exc)

    # S3 Storage requires a S3StorageCredential
    with pytest.raises(NotImplementedError) as exc:
        SparkSession(
            storage_credential=GCSStorageCredential(service_account_info={}), **spark_session_params
        )
    assert "Unsupported storage credential for S3: GCSStorageCredential" in str(exc)

    # Success
    SparkSession(
        storage_credential=S3StorageCredential(
            s3_access_key_id="test", s3_secret_access_key="test"
        ),
        **spark_session_params,
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
        storage_path="gs://test/",
        catalog_name="spark_catalog",
        schema_name="featurebyte",
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


@patch("featurebyte.session.spark.HiveConnection.__new__")
@pytest.mark.asyncio
async def test_get_table_details(config, spark_session_params):
    """
    Test snowflake session
    """
    session = SparkSession(
        storage_credential=S3StorageCredential(
            s3_access_key_id="test", s3_secret_access_key="test"
        ),
        **spark_session_params,
    )

    columns = ["col_name", "data_type", "comment"]
    rows = [
        ("id", "int", None),
        ("", "", ""),
        ("# Detailed Table Information", "", ""),
        ("Database", "featurebyte_20240110112131_971440", ""),
        ("Table", "test", ""),
        ("Owner", "runner", ""),
        ("Created Time", "Mon Jan 15 09:17:47 UTC 2024", ""),
        ("Last Access", "UNKNOWN", ""),
        ("Created By", "Spark 3.3.1", ""),
        ("Type", "MANAGED", ""),
        ("Provider", "hive", ""),
        ("Comment", "some desc", ""),
        ("Table Properties", "[transient_lastDdlTime=1705310267]", ""),
        (
            "Location",
            "file:/opt/spark/data/derby/warehouse/featurebyte_20240110112131_971440.db/test",
            "",
        ),
        ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", ""),
        ("InputFormat", "org.apache.hadoop.mapred.TextInputFormat", ""),
        ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", ""),
        ("Storage Properties", "[serialization.format=1]", ""),
        ("Partition Provider", "Catalog", ""),
    ]
    result = pd.DataFrame(rows, columns=columns)
    with patch("featurebyte.session.spark.SparkSession.execute_query") as mock_execute_query:
        mock_execute_query.return_value = result
        table_details = await session.get_table_details(
            database_name="spark_catalog",
            schema_name="featurebyte_20240110112131_971440",
            table_name="test",
        )
        assert table_details == TableDetails(
            details={
                "Database": "featurebyte_20240110112131_971440",
                "Table": "test",
                "Owner": "runner",
                "Created Time": "Mon Jan 15 09:17:47 UTC 2024",
                "Last Access": "UNKNOWN",
                "Created By": "Spark 3.3.1",
                "Type": "MANAGED",
                "Provider": "hive",
                "Comment": "some desc",
                "Table Properties": "[transient_lastDdlTime=1705310267]",
                "Location": "file:/opt/spark/data/derby/warehouse/featurebyte_20240110112131_971440.db/test",
                "Serde Library": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Storage Properties": "[serialization.format=1]",
                "Partition Provider": "Catalog",
            },
            fully_qualified_name="`spark_catalog`.`featurebyte_20240110112131_971440`.`test`",
        )
        assert table_details.description == "some desc"
