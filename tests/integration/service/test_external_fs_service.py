"""
Integration tests for ExternalDatabricksFeatureStoreService.
"""
import os
import time
from datetime import datetime

import pytest
from databricks import sql as databricks_sql

from featurebyte.service.external_feature_store import ExternalDatabricksFeatureStoreService


@pytest.fixture(name="external_fs_service")
def external_fs_service_fixture():
    """
    Fixture for ExternalFsService
    """
    return ExternalDatabricksFeatureStoreService()


@pytest.fixture(name="databricks_connection")
def databricks_connection_fixture():
    """
    Fixture for Databricks connection
    """
    connection = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_UNITY_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_ACCESS_TOKEN"],
    )

    test_schema_name = f"test_feature_store.testing_schema"

    datatime_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    feature_table_name = f"{test_schema_name}.feature_table_{datatime_str}"

    yield connection, feature_table_name

    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE {feature_table_name}")


def wait_for_job_to_complete(external_fs_service, job_id, wait_seconds=100):
    while wait_seconds > 0:
        if external_fs_service.is_job_running(job_id):
            time.sleep(1)
        else:
            break
        wait_seconds -= 1


def test_create_feature_table(databricks_connection, external_fs_service):
    """
    Test create_feature_table with schema
    """
    connection, feature_table_name = databricks_connection

    job_id = external_fs_service.create_feature_table(
        table_name=feature_table_name,
        primary_keys=["age", "deptId"],
        column_names=["age", "deptId", "gender"],
        column_types=["bigint", "bigint", "string"],
    )

    wait_for_job_to_complete(external_fs_service, job_id)

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {feature_table_name} LIMIT 1")
            data = cursor.fetchall()
            assert len(data) == 0
    finally:
        external_fs_service.jobs_api.delete_job(job_id)


def test_write_feature_table(databricks_connection, external_fs_service):
    """
    Test write_feature_table
    """
    _, feature_table_name = databricks_connection

    feature_sql = "select age, deptId, gender from test_feature_store.default.integration_testing_src where age <= 30"
    feature_column_names = ["gender"]
    feature_column_types = ["string"]
    job_id = external_fs_service.write_feature_table(
        table_name=feature_table_name,
        feature_sql=feature_sql,
        primary_keys=["age", "deptId"],
        feature_column_names=feature_column_names,
        feature_column_types=feature_column_types,
    )

    wait_for_job_to_complete(external_fs_service, job_id)

    try:
        df_features = external_fs_service.read_feature_table(table_name=feature_table_name)
        assert len(df_features) == 3
        assert set(df_features.columns) == {"age", "deptId", "gender"}
    finally:
        external_fs_service.jobs_api.delete_job(job_id)

    # test feature table merge with new feature
    feature_sql = "select age, deptId, name from test_feature_store.default.integration_testing_src where age >= 30"
    feature_column_names = ["name"]
    feature_column_types = ["string"]
    job_id = external_fs_service.write_feature_table(
        table_name=feature_table_name,
        feature_sql=feature_sql,
        primary_keys=["age", "deptId"],
        feature_column_names=feature_column_names,
        feature_column_types=feature_column_types,
    )

    wait_for_job_to_complete(external_fs_service, job_id)

    try:
        df_features = external_fs_service.read_feature_table(table_name=feature_table_name)
        assert len(df_features) == 6
        assert set(df_features.columns) == {"age", "deptId", "gender", "name"}
    finally:
        external_fs_service.jobs_api.delete_job(job_id)
