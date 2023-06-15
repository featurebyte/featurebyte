from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import FeatureList
from featurebyte.app import get_celery
from featurebyte.migration.service.data_warehouse import DataWarehouseMigrationServiceV8
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.service.working_schema import drop_all_objects
from tests.util.helper import (
    create_batch_request_table_from_dataframe,
    create_observation_table_from_dataframe,
    make_online_request,
)


@pytest.fixture(name="deployed_feature_list_deployment", scope="module")
def deployed_feature_list_and_deployment_fixture(event_table):
    """
    Fixture for a deployed feature list & deployment
    """
    event_view = event_table.get_view()
    event_view["ÀMOUNT"].fillna(0)
    feature_group = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="min",
        windows=["2h", "24h"],
        feature_names=["AMOUNT_MIN_2h", "AMOUNT_MIN_24h"],
    )
    features = FeatureList(
        [
            feature_group["AMOUNT_MIN_2h"],
            feature_group["AMOUNT_MIN_24h"],
        ],
        name="my_list_for_testing_schema_recreation",
    )
    features.save()

    next_job_datetime = pd.Timestamp("2001-01-02 12:00:00").to_pydatetime()
    with patch(
        "featurebyte.feature_manager.manager.get_next_job_datetime",
        return_value=next_job_datetime,
    ):
        deployment = features.deploy(make_production_ready=True)
        deployment.enable()

        yield features, deployment


@pytest.fixture(name="migration_service")
def migration_service_fixture(user, persistent, get_cred):
    """
    Fixture for DataWarehouseMigrationServiceV8
    """
    service = DataWarehouseMigrationServiceV8(
        user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    )
    service.set_credential_callback(get_cred)
    service.set_celery(get_celery())
    return service


@pytest.fixture
def patch_to_exclude_datasets(dataset_registration_helper):
    """
    Fixture to patch remove_materialized_tables to exclude dataset tables

    The dataset tables are stored in the metadata schema and will be dropped during the schema
    recreation process, but these datasets themselves are required to complete the process (when
    re-online-enabling features)
    """
    from featurebyte.session.base import BaseSchemaInitializer

    original_func = BaseSchemaInitializer.remove_materialized_tables

    def patched_remove_materialized_tables(table_names):
        known_tables = set([name.upper() for name in dataset_registration_helper.table_names])
        filtered_tables = []
        for table_name in table_names:
            if table_name.upper() in known_tables:
                continue
            filtered_tables.append(table_name)
        return original_func(filtered_tables)

    with patch("featurebyte.session.base.BaseSchemaInitializer.remove_materialized_tables") as p:
        p.side_effect = patched_remove_materialized_tables
        yield p


async def create_materialized_tables(session, data_source, feature_list, deployment):
    """
    Helper function to create a list of materialized tables
    """
    df = pd.DataFrame({"üser id": [1], "POINT_IN_TIME": pd.to_datetime(["2001-01-15 10:00:00"])})
    observation_table = await create_observation_table_from_dataframe(session, df, data_source)

    df = pd.DataFrame({"üser id": [1]})
    batch_request_table = await create_batch_request_table_from_dataframe(session, df, data_source)

    historical_feature_table = feature_list.compute_historical_feature_table(
        observation_table, str(ObjectId())
    )
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table, str(ObjectId())
    )

    return [observation_table, batch_request_table, historical_feature_table, batch_feature_table]


def check_materialized_tables(materialized_tables):
    """
    Helper function to check if the materialized tables still function correctly
    """
    for table in materialized_tables:
        df = table.preview()
        assert df.shape[0] > 0
        assert df.shape[1] > 0


@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.usefixtures("patch_to_exclude_datasets")
@pytest.mark.asyncio
async def test_drop_all_and_recreate(
    config,
    session,
    deployed_feature_list_deployment,
    migration_service,
    feature_store,
    source_type,
):
    """
    Test dropping all objects first then use WorkingSchemaService to restore it
    """
    snowflake_session = session
    deployed_feature_list, deployment = deployed_feature_list_deployment

    materialized_tables = await create_materialized_tables(
        session, feature_store.get_data_source(), deployed_feature_list, deployment
    )
    check_materialized_tables(materialized_tables)

    async def _get_object_counts():
        num_tables = len(await session.initializer().list_objects("TABLES"))
        num_functions = len(await session.initializer().list_objects("USER FUNCTIONS"))

        return num_tables, num_functions

    async def _get_schema_metadata():
        df = await snowflake_session.execute_query("SELECT * FROM METADATA_SCHEMA")
        return df.iloc[0].to_dict()

    entity_serving_names = [{"üser id": 1}]
    client = config.get_client()

    # Make an online request for reference
    res = make_online_request(client, deployment, entity_serving_names)
    assert res.status_code == 200
    expected_online_result = res.json()

    # Check current object counts
    init_num_tables, num_functions = await _get_object_counts()
    assert init_num_tables > 0
    assert num_functions > 0

    # Drop everything
    await drop_all_objects(snowflake_session)

    # Check objects are indeed dropped
    num_tables, num_functions = await _get_object_counts()
    assert num_tables < init_num_tables
    assert num_functions == 0

    # Check online requests can no longer be made
    res = make_online_request(client, deployment, entity_serving_names)
    assert res.status_code == 500
    if source_type == "snowflake":
        expected_error_message = "SQL compilation error"
    else:
        expected_error_message = "Table or view not found"
    assert expected_error_message in res.json()["detail"]

    # Recreate schema
    await migration_service.reset_working_schema(query_filter={"_id": ObjectId(feature_store.id)})

    # Check metadata are restored
    restored_metadata = await _get_schema_metadata()
    assert isinstance(restored_metadata["FEATURE_STORE_ID"], str)
    assert restored_metadata["MIGRATION_VERSION"] == 8

    # Check online request can be made and produces same result
    res = make_online_request(client, deployment, entity_serving_names)
    assert res.status_code == 200
    assert res.json() == expected_online_result

    # Check materialized tables still work
    check_materialized_tables(materialized_tables)
