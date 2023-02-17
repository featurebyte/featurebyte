from unittest.mock import Mock, patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import EventView, FeatureList
from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.migration.service.data_warehouse import DataWarehouseMigrationServiceV8
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.online_store import OnlineFeatureSpec
from featurebyte.schema.feature_list import FeatureListGetOnlineFeatures
from featurebyte.service.working_schema import drop_all_objects
from featurebyte.utils.credential import get_credential


@pytest.fixture(scope="session")
def user():
    """
    Mock user
    """
    user = Mock()
    user.id = ObjectId()
    return user


@pytest.fixture(name="deployed_feature_list", scope="module")
def deployed_feature_list_fixture(event_data):
    """
    Fixture for a deployed feature list
    """
    event_view = EventView.from_event_data(event_data)
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
        features.deploy(make_production_ready=True, enable=True)

        yield features


def make_online_request(client, feature_list, entity_serving_names):
    """
    Helper function to make an online request via REST API
    """
    data = FeatureListGetOnlineFeatures(entity_serving_names=entity_serving_names)
    res = client.post(
        f"/feature_list/{str(feature_list.id)}/online_features",
        json=data.json_dict(),
    )
    return res


@pytest.fixture(name="migration_service")
def migration_service_fixture(user, persistent):
    """
    Fixture for DataWarehouseMigrationServiceV8
    """
    service = DataWarehouseMigrationServiceV8(
        user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
    )
    service.set_credential_callback(get_credential)
    return service


@pytest_asyncio.fixture
async def patch_list_tables_to_exclude_datasets(session, dataset_registration_helper):
    """
    Fixture to patch list_tables to exclude dataset tables

    The dataset tables are stored in the metadata schema and will be dropped during the schema
    recreation process, but these datasets themselves are required to complete the process (when
    re-online-enabling features)
    """
    assert session.source_type == "snowflake"

    async def patched_list_tables(database_name=None, schema_name=None):
        tables = (
            await session.execute_query(f'SHOW TABLES IN SCHEMA "{database_name}"."{schema_name}"')
        )["name"].tolist()
        known_tables = set(dataset_registration_helper.table_names)
        tables = [t for t in tables if t not in known_tables]
        return tables

    with patch("featurebyte.session.snowflake.SnowflakeSession.list_tables") as p:
        p.side_effect = patched_list_tables
        yield p


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.usefixtures("patch_list_tables_to_exclude_datasets")
@pytest.mark.asyncio
async def test_drop_all_and_recreate(
    config,
    session,
    deployed_feature_list,
    migration_service,
    feature_store,
):
    """
    Test dropping all objects first then use WorkingSchemaService to restore it
    """
    snowflake_session = session

    async def _list_objects(obj):
        query = f"SHOW {obj} IN {snowflake_session.database_name}.{snowflake_session.schema_name}"
        return await snowflake_session.execute_query(query)

    async def _get_object_counts():
        num_tables = len(await _list_objects("TABLES"))
        num_functions = len(await _list_objects("USER FUNCTIONS"))
        num_procedures = len(await _list_objects("USER PROCEDURES"))
        num_tasks = len(await _list_objects("TASKS"))
        return num_tables, num_functions, num_procedures, num_tasks

    async def _get_tasks():
        df = await _list_objects("TASKS")
        # Filter tasks by aggregation ids of the feature list of this current test only (other
        # integration tests might manually schedule tasks without only enabling a corresponding
        # feature, and those tasks should excluded in the checks below)
        feature_agg_ids = set()
        for feature_name in deployed_feature_list.feature_names:
            online_spec = OnlineFeatureSpec(
                feature=ExtendedFeatureModel(**deployed_feature_list[feature_name].dict())
            )
            feature_agg_ids.update([agg_id.upper() for agg_id in online_spec.aggregation_ids])
        task_names = set(df["name"].str.upper().tolist())
        task_names = task_names.intersection(feature_agg_ids)
        return sorted(task_names)

    async def _get_schema_metadata():
        df = await snowflake_session.execute_query("SELECT * FROM METADATA_SCHEMA")
        return df.iloc[0].to_dict()

    entity_serving_names = [{"üser id": 1}]
    client = config.get_client()

    # Make an online request for reference
    res = make_online_request(client, deployed_feature_list, entity_serving_names)
    assert res.status_code == 200
    expected_online_result = res.json()
    original_tasks = await _get_tasks()

    # Check current object counts
    init_num_tables, num_functions, num_procedures, num_tasks = await _get_object_counts()
    assert init_num_tables > 0
    assert num_functions > 0
    assert num_procedures > 0
    assert num_tasks > 0

    # Drop everything
    await drop_all_objects(snowflake_session)

    # Check objects are indeed dropped
    num_tables, num_functions, num_procedures, num_tasks = await _get_object_counts()
    assert num_tables < init_num_tables
    assert num_functions == 0
    assert num_procedures == 0
    assert num_tasks == 0

    # Check online requests can no longer be made
    res = make_online_request(client, deployed_feature_list, entity_serving_names)
    assert res.status_code == 500
    assert "SQL compilation error" in res.json()["detail"]

    await migration_service.reset_working_schema(query_filter={"_id": ObjectId(feature_store.id)})

    # Check tasks and metadata are restored
    restored_tasks = await _get_tasks()
    restored_metadata = await _get_schema_metadata()
    assert len(restored_tasks) == len(original_tasks)
    assert isinstance(restored_metadata["FEATURE_STORE_ID"], str)
    assert restored_metadata["MIGRATION_VERSION"] == 8

    # Check online request can be made and produces same result
    res = make_online_request(client, deployed_feature_list, entity_serving_names)
    assert res.status_code == 200
    assert res.json() == expected_online_result
