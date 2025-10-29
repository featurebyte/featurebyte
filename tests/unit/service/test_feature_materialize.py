"""
Test FeatureMaterializeService
"""

from dataclasses import asdict
from datetime import datetime
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.models.feature_materialize_run import FeatureMaterializeRun
from featurebyte.models.offline_store_feature_table import (
    OfflineLastMaterializedAtUpdate,
    OnlineStoreLastMaterializedAt,
)
from featurebyte.models.precomputed_lookup_feature_table import get_lookup_steps_unique_identifier
from featurebyte.schema.catalog import CatalogOnlineStoreUpdate
from featurebyte.schema.worker.task.deployment_create_update import CreateDeploymentPayload
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    deploy_feature_ids,
    extract_session_executed_queries,
    get_relationship_info,
    safe_freeze_time,
)


@pytest.fixture(name="patched_validate_row_index", autouse=True)
def patched_validate_row_index_fixture():
    """
    Patched validate_output_row_index to be a no-op
    """
    with patch("featurebyte.session.session_helper.validate_output_row_index") as patched:
        yield patched


@pytest.fixture(name="mock_get_feature_store_session")
def mock_get_feature_store_session_fixture(mock_snowflake_session):
    """
    Patch get_feature_store_session to return a mock session
    """
    with patch(
        "featurebyte.service.feature_materialize.SessionManagerService.get_feature_store_session",
    ) as patched_get_feature_store_session:
        patched_get_feature_store_session.return_value = mock_snowflake_session
        yield patched_get_feature_store_session


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    app_container,
    production_ready_feature_list,
    online_store,
    mock_update_data_warehouse,
    is_online_store_registered_for_catalog,
    populate_offline_feature_tables_for_catalog,
):
    """
    Fixture for FeatureMaterializeService
    """
    _ = mock_update_data_warehouse

    if is_online_store_registered_for_catalog:
        catalog_update = CatalogOnlineStoreUpdate(online_store_id=online_store.id)
        await app_container.catalog_service.update_document(
            document_id=production_ready_feature_list.catalog_id, data=catalog_update
        )

    if populate_offline_feature_tables_for_catalog:
        catalog_update = CatalogOnlineStoreUpdate(populate_offline_feature_tables=True)
        await app_container.catalog_service.update_document(
            document_id=app_container.catalog_id, data=catalog_update
        )

    # TODO: use deploy_feature() helper
    deployment_id = ObjectId()
    with (
        patch(
            "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService.initialize_new_columns"
        ),
        patch(
            "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService.initialize_precomputed_lookup_feature_table"
        ),
    ):
        await app_container.deploy_service.create_deployment(
            deployment_id=deployment_id,
            payload=CreateDeploymentPayload(
                feature_list_id=production_ready_feature_list.id,
                enabled=True,
            ),
        )

    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    yield deployed_feature_list


@pytest_asyncio.fixture
async def deployed_feature_list_composite_entity(
    app_container,
    float_feature_composite_entity,
    float_feature_composite_entity_v2,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a feature list with composite entity
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    float_feature_composite_entity.save()
    float_feature_composite_entity_v2.save()
    feature_list_model = await deploy_feature_ids(
        app_container,
        "my_list",
        [
            float_feature_composite_entity.id,
            float_feature_composite_entity_v2.id,
        ],
    )
    return feature_list_model


@pytest_asyncio.fixture
async def deployed_feature_list_no_entity(
    app_container,
    feature_without_entity,
    mock_deployment_flow,
):
    """
    Fixture for a feature list with no entity
    """
    _ = mock_deployment_flow

    feature_without_entity.save()
    feature_list_model = await deploy_feature_ids(
        app_container, "my_list", [feature_without_entity.id]
    )
    return feature_list_model


@pytest_asyncio.fixture
async def deployed_feature_list_internal_relationships(
    app_container,
    feature_with_internal_parent_child_relationships,
    mock_deployment_flow,
):
    """
    Fixture for a feature list with internal relationships
    """
    _ = mock_deployment_flow

    feature_with_internal_parent_child_relationships.save()
    feature_list_model = await deploy_feature_ids(
        app_container, "my_list", [feature_with_internal_parent_child_relationships.id]
    )
    return feature_list_model


@pytest_asyncio.fixture(name="deployed_feature")
async def deployed_feature_fixture(feature_service, deployed_feature_list):
    """
    Fixture for a deployed feature
    """
    return await feature_service.get_document(deployed_feature_list.feature_ids[0])


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def offline_store_feature_table_fixture(app_container, deployed_feature):
    """
    Fixture for offline store feature table
    """
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"feature_ids": deployed_feature.id}
    ):
        return model


@pytest_asyncio.fixture(name="offline_store_feature_table_composite_entity")
async def offline_store_feature_table_composite_entity_fixture(
    app_container, deployed_feature_list_composite_entity
):
    """
    Fixture for offline store feature table with composite entity
    """
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"feature_ids": deployed_feature_list_composite_entity.feature_ids}
    ):
        return model


@pytest_asyncio.fixture(name="offline_store_feature_table_no_entity")
async def offline_store_feature_table_no_entity_fixture(
    app_container, deployed_feature_list_no_entity
):
    """
    Fixture for offline store feature table with no entity
    """
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"feature_ids": deployed_feature_list_no_entity.feature_ids}
    ):
        return model


@pytest_asyncio.fixture(name="offline_store_feature_table_internal_relationships")
async def offline_store_feature_table_internal_relationships_fixture(
    app_container, deployed_feature_list_internal_relationships
):
    """
    Fixture for offline store feature table with internal relationships
    """
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"feature_ids": deployed_feature_list_internal_relationships.feature_ids}
    ):
        return model


@pytest_asyncio.fixture(name="offline_store_feature_table_with_precomputed_lookup")
async def offline_store_feature_table_with_precomputed_lookup_fixture(
    app_container,
    deployed_feature_list_requiring_parent_serving,
    gender_entity_id,
):
    """
    Fixture for offline store feature table that requires precomputed lookup (this returns the
    source feature table)
    """
    _ = deployed_feature_list_requiring_parent_serving
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"primary_entity_ids": gender_entity_id}
    ):
        return model


@pytest_asyncio.fixture(name="offline_store_feature_table_with_precomputed_lookup_ttl")
async def offline_store_feature_table_with_precomputed_lookup_ttl_fixture(
    app_container,
    deployed_feature_list_requiring_parent_serving_ttl,
    cust_id_entity,
):
    """
    Fixture for offline store feature table that requires precomputed lookup (this returns the
    source feature table)
    """
    _ = deployed_feature_list_requiring_parent_serving_ttl
    async for model in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={"primary_entity_ids": cust_id_entity.id}
    ):
        return model


@pytest_asyncio.fixture(name="feast_feature_store")
async def feast_feature_store_fixture(
    app_container, offline_store_feature_table, insert_credential
):
    """
    Fixture for the feast feature store
    """
    deployment_id = offline_store_feature_table.deployment_ids[0]
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    feast_store_service = app_container.feast_feature_store_service
    return await feast_store_service.get_feast_feature_store_for_deployment(deployment=deployment)


@pytest.fixture(name="mock_materialize_partial")
def mock_materialize_partial_fixture():
    """
    Patch materialize_partial method
    """
    with patch(
        "featurebyte.service.feature_materialize.materialize_partial",
    ) as patched_materialize_partial:
        yield patched_materialize_partial


@pytest.fixture(name="mocked_unique_identifier_generator", autouse=True)
def mocked_unique_identifier_generator_fixture():
    """
    Patch ObjectId to return a fixed value so that queries are deterministic
    """
    mocked_object_id = ObjectId("000000000000000000000000")
    with patch("featurebyte.service.feature_materialize.ObjectId") as patched_object_id:
        patched_object_id.return_value = mocked_object_id
        yield


@pytest.fixture(name="freeze_feature_timestamp", autouse=True)
def freeze_feature_timestamp_fixture():
    """
    Patch ObjectId to return a fixed value so that queries are deterministic
    """
    with safe_freeze_time("2022-01-01 00:00:00"):
        yield


@pytest_asyncio.fixture(name="feature_materialize_run")
async def feature_materialize_run_fixture(app_container):
    """
    Fixture for a FeatureMaterializeRun
    """
    return await app_container.feature_materialize_run_service.create_document(
        FeatureMaterializeRun(
            offline_store_feature_table_id=ObjectId(),
            scheduled_job_ts=datetime(2022, 1, 1, 0, 0),
        )
    )


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_materialize_features(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    update_fixtures,
):
    """
    Test materialize_features
    """
    async with feature_materialize_service.materialize_features(
        feature_table_model=offline_store_feature_table,
    ) as materialized_features_set:
        pass

    assert len(mock_snowflake_session.execute_query_long_running.call_args_list) == 4

    table_name = "cat1_cust_id_30m"
    assert list(materialized_features_set.all_materialized_features.keys()) == [table_name]

    materialized_features = materialized_features_set.all_materialized_features[table_name]
    materialized_features_dict = asdict(materialized_features)
    materialized_features_dict["materialized_table_name"], suffix = materialized_features_dict[
        "materialized_table_name"
    ].rsplit("_", 1)
    assert suffix != ""
    assert materialized_features_dict == {
        "materialized_table_name": "TEMP_FEATURE_TABLE",
        "column_names": [f"sum_30m_{get_version()}"],
        "data_types": ["FLOAT"],
        "feature_timestamp": datetime(2022, 1, 1, 0, 0),
        "serving_names": ["cust_id"],
        "source_type": "snowflake",
    }

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/materialize_features_queries.sql",
        update_fixtures,
    )

    # Check that the temporary tables are dropped
    assert mock_snowflake_session.drop_table.call_args_list == [
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
            timeout=86400,
        ),
        call(
            table_name="TEMP_REQUEST_TABLE_000000000000000000000000",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
        call(
            table_name="TEMP_FEATURE_TABLE_000000000000000000000000",
            schema_name="sf_schema",
            database_name="sf_db",
            if_exists=True,
        ),
    ]


@pytest.mark.parametrize("is_online_store_registered_for_catalog", [True, False])
@pytest.mark.parametrize("populate_offline_feature_tables_for_catalog", [True, False])
@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    is_online_store_registered_for_catalog,
    populate_offline_feature_tables_for_catalog,
    feature_materialize_run,
    update_fixtures,
    insert_credential,
):
    """
    Test scheduled_materialize_features
    """
    await feature_materialize_service.scheduled_materialize_features(
        offline_store_feature_table, feature_materialize_run.id
    )

    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    if (
        not is_online_store_registered_for_catalog
        and not populate_offline_feature_tables_for_catalog
    ):
        assert executed_queries == ""
    else:
        assert_equal_with_expected_fixture(
            executed_queries,
            "tests/fixtures/feature_materialize/scheduled_materialize_features_queries.sql",
            update_fixtures,
        )

    # Check online materialization called if there is a registered online store (note: start_date is
    # None because the initialize_new_columns is mocked when deploying)
    if is_online_store_registered_for_catalog:
        _, kwargs = mock_materialize_partial.call_args
        _ = kwargs.pop("feature_store")
        feature_view = kwargs.pop("feature_view")
        assert feature_view.name == "cat1_cust_id_30m"
        assert kwargs == {
            "session": mock_snowflake_session,
            "columns": [f"sum_30m_{get_version()}"],
            "start_date": None,
            "end_date": datetime(2022, 1, 1, 0, 0),
            "with_feature_timestamp": True,
        }
    else:
        assert mock_materialize_partial.call_count == 0

    # Check last materialization timestamp updated
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    if populate_offline_feature_tables_for_catalog or is_online_store_registered_for_catalog:
        assert updated_feature_table.last_materialized_at == datetime(2022, 1, 1, 0, 0)
    else:
        assert updated_feature_table.last_materialized_at is None

    # Check online last materialization timestamp updated
    if is_online_store_registered_for_catalog:
        assert len(updated_feature_table.online_stores_last_materialized_at) == 1
        assert updated_feature_table.online_stores_last_materialized_at[0].value == datetime(
            2022, 1, 1, 0, 0
        )
    else:
        assert len(updated_feature_table.online_stores_last_materialized_at) == 0

    # Check FeatureMaterializeRun record is updated
    feature_materialize_run = await app_container.feature_materialize_run_service.get_document(
        feature_materialize_run.id
    )
    assert feature_materialize_run.feature_materialize_ts is not None
    assert feature_materialize_run.completion_ts is not None
    assert feature_materialize_run.completion_status == "success"


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features_if_materialized_before(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    online_store,
    mock_materialize_partial,
    insert_credential,
):
    """
    Test calling scheduled_materialize_features when the feature table has already been materialized
    before. When calling online materialize it should provide the feature table's
    last_materialized_at as start date.
    """
    offline_store_feature_table.online_stores_last_materialized_at = [
        OnlineStoreLastMaterializedAt(
            online_store_id=online_store.id,
            value=datetime(2022, 1, 1, 0, 0),
        )
    ]

    with safe_freeze_time("2022-01-02 00:00:00"):
        await feature_materialize_service.scheduled_materialize_features(
            offline_store_feature_table
        )

    # Check online materialization
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
        "session": mock_snowflake_session,
        "columns": [f"sum_30m_{get_version()}"],
        "start_date": datetime(2022, 1, 1, 0, 0),
        "end_date": datetime(2022, 1, 2, 0, 0),
        "with_feature_timestamp": True,
    }

    # Check offline last materialization timestamp updated
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    assert updated_feature_table.last_materialized_at == datetime(2022, 1, 2, 0, 0)

    # Check online last materialization timestamp updated
    offline_store_feature_table.online_stores_last_materialized_at = [
        OnlineStoreLastMaterializedAt(
            online_store_id=online_store.id,
            value=datetime(2022, 1, 2, 0, 0),
        )
    ]


@pytest.mark.asyncio
async def test_scheduled_materialize_features_batch_columns(
    app_container,
    feature_materialize_service,
    offline_store_feature_table_with_precomputed_lookup,
    mock_get_feature_store_session,
    mock_snowflake_session,
    online_store,
    mock_materialize_partial,
):
    """
    Test calls to materialize_partial are batched
    """
    _ = mock_get_feature_store_session

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        schema = {
            f"__feature_requiring_parent_serving_{get_version()}__part1": "some_info",
            f"__feature_requiring_parent_serving_plus_123_{get_version()}__part1": "some_info",
        }
        return schema

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema

    offline_store_feature_table_with_precomputed_lookup.online_stores_last_materialized_at = [
        OnlineStoreLastMaterializedAt(
            online_store_id=online_store.id,
            value=datetime(2022, 1, 1, 0, 0),
        )
    ]

    with safe_freeze_time("2022-01-02 00:00:00"):
        with patch("featurebyte.service.feature_materialize.NUM_COLUMNS_PER_MATERIALIZE", 1):
            await feature_materialize_service.scheduled_materialize_features(
                offline_store_feature_table_with_precomputed_lookup
            )

    # Check online materialization for a feature view
    call_args_list = [
        arg
        for arg in mock_materialize_partial.call_args_list
        if arg[1]["feature_view"].name == "cat1_gender_1d"
    ]
    assert len(call_args_list) == 2

    expected_columns = [
        [f"__feature_requiring_parent_serving_{get_version()}__part1"],
        [f"__feature_requiring_parent_serving_plus_123_{get_version()}__part1"],
    ]
    for (_, kwargs), columns in zip(call_args_list, expected_columns):
        _ = kwargs.pop("feature_store")
        kwargs.pop("feature_view")
        assert kwargs == {
            "session": mock_snowflake_session,
            "columns": columns,
            "start_date": datetime(2022, 1, 1, 0, 0),
            "end_date": datetime(2022, 1, 2, 0, 0),
            "with_feature_timestamp": False,
        }

    # Check offline last materialization timestamp updated
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table_with_precomputed_lookup.id
    )
    assert updated_feature_table.last_materialized_at == datetime(2022, 1, 2, 0, 0)

    # Check online last materialization timestamp updated
    offline_store_feature_table_with_precomputed_lookup.online_stores_last_materialized_at = [
        OnlineStoreLastMaterializedAt(
            online_store_id=online_store.id,
            value=datetime(2022, 1, 2, 0, 0),
        )
    ]


@pytest.mark.parametrize("is_online_store_registered_for_catalog", [True, False])
@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__table_does_not_exist(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    is_online_store_registered_for_catalog,
    update_fixtures,
    insert_credential,
):
    """
    Test initialize_new_columns when feature table is not yet created
    """

    def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            raise ValueError()

    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_new_table.sql",
        update_fixtures,
    )

    if is_online_store_registered_for_catalog:
        _, kwargs = mock_materialize_partial.call_args
        _ = kwargs.pop("feature_store")
        feature_view = kwargs.pop("feature_view")
        assert feature_view.name == "cat1_cust_id_30m"
        assert kwargs == {
            "session": mock_snowflake_session,
            "columns": [f"sum_30m_{get_version()}"],
            "start_date": None,
            "end_date": datetime(2022, 1, 1, 0, 0),
            "with_feature_timestamp": True,
        }
    else:
        assert mock_materialize_partial.call_count == 0


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__table_exists(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    update_fixtures,
    insert_credential,
):
    """
    Test initialize_new_columns when feature table already exists
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"some_existing_col": "some_info"}

    async def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            return pd.DataFrame({"RESULT": [10]})
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": "2022-10-15 10:00:00"}])

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_existing_table.sql",
        update_fixtures,
    )

    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
        "session": mock_snowflake_session,
        "columns": [f"sum_30m_{get_version()}"],
        "end_date": datetime(2022, 10, 15, 10, 0, 0),
        "start_date": None,
        "with_feature_timestamp": True,
    }


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__table_exists_but_empty(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    update_fixtures,
    insert_credential,
):
    """
    Test initialize_new_columns when feature table already exists but empty
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"some_existing_col": "some_info"}

    async def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            return pd.DataFrame({"RESULT": [0]})

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_existing_table_empty.sql",
        update_fixtures,
    )

    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
        "session": mock_snowflake_session,
        "columns": [f"sum_30m_{get_version()}"],
        "end_date": datetime(2022, 1, 1, 0, 0, 0),
        "start_date": None,
        "with_feature_timestamp": True,
    }


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__no_feature_columns(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    update_fixtures,
    insert_credential,
):
    """
    Test initialize_new_columns when feature table doesn't have any feature columns
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"some_existing_col": "some_info"}

    async def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            return pd.DataFrame({"RESULT": [10]})
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": "2022-10-15 10:00:00"}])

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    offline_store_feature_table.output_column_names = []
    offline_store_feature_table.output_dtypes = []

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_no_feature_columns.sql",
        update_fixtures,
    )

    mock_materialize_partial.assert_not_called()


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__databricks_unity(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    databricks_source_info,
    update_fixtures,
    insert_credential,
):
    """
    Test initialize_new_columns when session is databricks_unity
    """

    def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            raise ValueError()

    mock_snowflake_session.source_type = "databricks_unity"
    mock_snowflake_session.get_source_info.return_value = databricks_source_info
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_new_table_databricks.sql",
        update_fixtures,
    )

    assert mock_materialize_partial.call_count == 1


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_drop_columns(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    update_fixtures,
):
    """
    Test drop_columns
    """
    await feature_materialize_service.drop_columns(offline_store_feature_table, ["a", "b"])
    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/drop_columns.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_drop_table(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
):
    """
    Test drop_columns
    """
    await feature_materialize_service.drop_table(offline_store_feature_table)
    assert mock_snowflake_session.drop_table.call_args == call(
        offline_store_feature_table.name,
        schema_name="sf_schema",
        database_name="sf_db",
        if_exists=True,
    )


@pytest.mark.parametrize("use_batched_feature_query_set", [False, True])
@pytest.mark.asyncio
async def test_materialize_features_composite_entity(
    offline_store_feature_table_composite_entity,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    use_batched_feature_query_set,
    update_fixtures,
):
    """
    Test materialize_features for a feature table with composite entity
    """
    _ = mock_get_feature_store_session

    if use_batched_feature_query_set:
        # Set to a small number to force splitting
        num_features_per_query = 1
        fixture_filename = "tests/fixtures/feature_materialize/materialize_features_queries_composite_entity_batch.sql"
    else:
        num_features_per_query = 20
        fixture_filename = (
            "tests/fixtures/feature_materialize/materialize_features_queries_composite_entity.sql"
        )

    with patch("featurebyte.session.session_helper.NUM_FEATURES_PER_QUERY", num_features_per_query):
        async with feature_materialize_service.materialize_features(
            feature_table_model=offline_store_feature_table_composite_entity,
        ) as materialized_features_set:
            pass

    table_name = "cat1_cust_id_another_key_30m"
    assert list(materialized_features_set.all_materialized_features.keys()) == [table_name]

    # Check concatenated column generated
    materialized_features = materialized_features_set.all_materialized_features[table_name]
    assert materialized_features.serving_names_and_column_names == [
        "cust_id",
        "another_key",
        "cust_id x another_key",
        f"composite_entity_feature_1d_{get_version()}",
        f"composite_entity_feature_1d_plus_123_{get_version()}",
    ]

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        fixture_filename,
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_materialize_features_no_entity_databricks_unity(
    offline_store_feature_table_no_entity,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    databricks_source_info,
    update_fixtures,
):
    """
    Test creating a feature table with no entity sets the primary constraint on dummy column
    """
    _ = mock_get_feature_store_session

    def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)" in query:
            raise ValueError()

    mock_snowflake_session.source_type = "databricks_unity"
    mock_snowflake_session.get_source_info.return_value = databricks_source_info
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    patch.stopall()  # stop the patcher on initialize_new_columns()
    await feature_materialize_service.initialize_new_columns(offline_store_feature_table_no_entity)

    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_features_no_entity_databricks_unity.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_update_online_store__never_materialized_before(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    feast_feature_store,
):
    """
    Test update_online_store for a new online store
    """
    offline_last_materialized_at = datetime(2022, 10, 15, 10, 0, 0)

    async def mock_execute_query(query, **kwargs):
        _ = kwargs
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": offline_last_materialized_at.isoformat()}])

    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    offline_store_feature_table.last_materialized_at = offline_last_materialized_at
    offline_store_feature_table.online_stores_last_materialized_at = []
    await feature_materialize_service.update_online_store(
        online_store_id=feast_feature_store.online_store_id,
        feature_table_model=offline_store_feature_table,
        session=mock_snowflake_session,
    )

    # Check materialized_partial called correctly
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
        "session": mock_snowflake_session,
        "columns": [f"sum_30m_{get_version()}"],
        "end_date": offline_last_materialized_at,
        "start_date": None,
        "with_feature_timestamp": True,
    }

    # Check online last updated correctly
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    assert updated_feature_table.online_stores_last_materialized_at == [
        OnlineStoreLastMaterializedAt(
            online_store_id=feast_feature_store.online_store_id,
            value=offline_last_materialized_at,
        )
    ]


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_update_online_store__materialized_before(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    feast_feature_store,
):
    """
    Test update_online_store when the online store was materialized before but is now outdated (e.g.
    it was detached from the catalog for a while)
    """
    offline_last_materialized_at = datetime(2022, 12, 15, 10, 0, 0)
    online_last_materialized_at = datetime(2022, 10, 15, 10, 0, 0)

    async def mock_execute_query(query, **kwargs):
        _ = kwargs
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": offline_last_materialized_at.isoformat()}])

    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query

    offline_store_feature_table.last_materialized_at = offline_last_materialized_at
    offline_store_feature_table.online_stores_last_materialized_at = [
        OnlineStoreLastMaterializedAt(
            online_store_id=feast_feature_store.online_store_id,
            value=online_last_materialized_at,
        )
    ]
    await feature_materialize_service.update_online_store(
        online_store_id=feast_feature_store.online_store_id,
        feature_table_model=offline_store_feature_table,
        session=mock_snowflake_session,
    )

    # Check materialized_partial called correctly
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
        "session": mock_snowflake_session,
        "columns": [f"sum_30m_{get_version()}"],
        "end_date": offline_last_materialized_at,
        "start_date": online_last_materialized_at,
        "with_feature_timestamp": True,
    }

    # Check online last updated correctly
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    assert updated_feature_table.online_stores_last_materialized_at == [
        OnlineStoreLastMaterializedAt(
            online_store_id=feast_feature_store.online_store_id,
            value=offline_last_materialized_at,
        )
    ]


@pytest.mark.asyncio
async def test_materialize_features_internal_relationships(
    app_container,
    offline_store_feature_table_internal_relationships,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
    cust_id_entity,
    gender_entity,
):
    """
    Test materialize_features for a feature table with internal relationships. The sql query should
    be doing feature specific parent entity lookup.
    """
    _ = mock_get_feature_store_session

    async with feature_materialize_service.materialize_features(
        feature_table_model=offline_store_feature_table_internal_relationships,
    ):
        pass

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    # Remove dynamic fields (relationship info id appears in the generated sql due to the use of
    # frozen relationships)
    relationship_info_id = (
        await get_relationship_info(
            app_container,
            child_entity_id=cust_id_entity.id,
            parent_entity_id=gender_entity.id,
        )
    ).id
    executed_queries = executed_queries.replace(str(relationship_info_id), "0" * 24)

    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/materialize_features_queries_internal_relationships.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_materialize_partial")
@pytest.mark.asyncio
async def test_precomputed_lookup_feature_table__initialize_new_columns(
    app_container,
    offline_store_feature_table_with_precomputed_lookup,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
    cust_id_entity,
    gender_entity,
):
    """
    Test initialize_new_columns when a feature list requires parent entity serving
    """
    _ = mock_get_feature_store_session

    def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)\nFROM" in query:
            raise ValueError()

    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    # stop the patcher on initialize_new_columns(), needed because
    # offline_store_feature_table_with_precomputed_lookup's feature fixture patched it.
    patch.stopall()
    await feature_materialize_service.initialize_new_columns(
        feature_table_model=offline_store_feature_table_with_precomputed_lookup,
    )

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    # Remove dynamic fields (appears in the name of the precomputed lookup feature table)
    relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=cust_id_entity.id,
        parent_entity_id=gender_entity.id,
    )
    expected_suffix = get_lookup_steps_unique_identifier([relationship_info])
    executed_queries = executed_queries.replace(expected_suffix, "0" * 6)

    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/initialize_new_columns_precomputed_lookup.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_materialize_partial")
@pytest.mark.asyncio
async def test_precomputed_lookup_feature_table__scheduled_materialize_features(
    app_container,
    offline_store_feature_table_with_precomputed_lookup,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
    cust_id_entity,
    gender_entity,
):
    """
    Test scheduled_materialize_features when a feature list requires parent entity serving
    """
    _ = mock_get_feature_store_session

    # Simulate previous materialize date
    service = app_container.offline_store_feature_table_service
    update_schema = OfflineLastMaterializedAtUpdate(
        last_materialized_at=datetime(2022, 1, 5),
    )
    offline_store_feature_table_with_precomputed_lookup = await service.update_document(
        document_id=offline_store_feature_table_with_precomputed_lookup.id, data=update_schema
    )
    async for table in service.list_precomputed_lookup_feature_tables_from_source(
        offline_store_feature_table_with_precomputed_lookup.id
    ):
        await service.update_document(document_id=table.id, data=update_schema)

    # Run scheduled materialize at a later date
    with safe_freeze_time(datetime(2022, 1, 6)):
        await feature_materialize_service.scheduled_materialize_features(
            feature_table_model=offline_store_feature_table_with_precomputed_lookup,
        )

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    # Remove dynamic fields (appears in the name of the precomputed lookup feature table)
    relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=cust_id_entity.id,
        parent_entity_id=gender_entity.id,
    )
    expected_suffix = get_lookup_steps_unique_identifier([relationship_info])
    executed_queries = executed_queries.replace(expected_suffix, "0" * 6)

    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/scheduled_materialize_features_precomputed_lookup.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_materialize_partial")
@pytest.mark.asyncio
async def test_precomputed_lookup_feature_table__scheduled_materialize_features_ttl(
    app_container,
    offline_store_feature_table_with_precomputed_lookup_ttl,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
    cust_id_entity,
    transaction_entity,
):
    """
    Test scheduled_materialize_features when a feature list requires parent entity serving where the
    parent feature has ttl. In this case, the lookup entity universe should not be using last materialized
    timestamp.
    """
    _ = mock_get_feature_store_session

    # Simulate previous materialize date
    service = app_container.offline_store_feature_table_service
    update_schema = OfflineLastMaterializedAtUpdate(
        last_materialized_at=datetime(2022, 1, 5),
    )
    offline_store_feature_table_with_precomputed_lookup = await service.update_document(
        document_id=offline_store_feature_table_with_precomputed_lookup_ttl.id, data=update_schema
    )
    async for table in service.list_precomputed_lookup_feature_tables_from_source(
        offline_store_feature_table_with_precomputed_lookup.id
    ):
        await service.update_document(document_id=table.id, data=update_schema)

    # Run scheduled materialize at a later date
    with safe_freeze_time(datetime(2022, 1, 6)):
        await feature_materialize_service.scheduled_materialize_features(
            feature_table_model=offline_store_feature_table_with_precomputed_lookup,
        )

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    # Remove dynamic fields (appears in the name of the precomputed lookup feature table)
    relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=transaction_entity.id,
        parent_entity_id=cust_id_entity.id,
    )
    expected_suffix = get_lookup_steps_unique_identifier([relationship_info])
    executed_queries = executed_queries.replace(expected_suffix, "0" * 6)

    # The start date should not be of the lookup entity universe should not be datetime(2022, 1, 5)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/scheduled_materialize_features_precomputed_lookup_ttl.sql",
        update_fixtures,
    )


@pytest.mark.usefixtures("mock_materialize_partial")
@pytest.mark.parametrize("has_missing_column", [False, True])
@pytest.mark.asyncio
async def test_precomputed_lookup_feature_table__initialize_new_table(
    app_container,
    offline_store_feature_table_with_precomputed_lookup,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
    cust_id_entity,
    gender_entity,
    has_missing_column,
):
    """
    Test initialize_precomputed_lookup_feature_table
    """
    _ = mock_get_feature_store_session

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        schema = {f"__feature_requiring_parent_serving_{get_version()}__part1": "some_info"}
        if not has_missing_column:
            schema[f"__feature_requiring_parent_serving_plus_123_{get_version()}__part1"] = (
                "some_info"
            )
        return schema

    def mock_execute_query(query, **kwargs):
        _ = kwargs
        if "COUNT(*)\nFROM" in query:
            # Simulate that source feature table exists and lookup feature table doesn't
            table_name = query.split("FROM", 1)[1].strip().replace('"', "")
            if table_name == "cat1_gender_1d":
                return pd.DataFrame({"RESULT": [10]})
            raise ValueError()
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": datetime(2022, 1, 5).isoformat()}])
        return None

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema
    mock_snowflake_session.execute_query_long_running.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    service = app_container.offline_store_feature_table_service
    source_feature_table = offline_store_feature_table_with_precomputed_lookup
    lookup_feature_tables = [
        doc
        async for doc in service.list_precomputed_lookup_feature_tables_from_source(
            source_feature_table.id
        )
    ]

    # stop the patcher on initialize_new_columns(), needed because
    # offline_store_feature_table_with_precomputed_lookup's feature fixture patched it.
    patch.stopall()
    await feature_materialize_service.initialize_precomputed_lookup_feature_table(
        source_feature_table.id, lookup_feature_tables
    )

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)

    # Remove dynamic fields (appears in the name of the precomputed lookup feature table)
    relationship_info = await get_relationship_info(
        app_container,
        child_entity_id=cust_id_entity.id,
        parent_entity_id=gender_entity.id,
    )
    expected_suffix = get_lookup_steps_unique_identifier([relationship_info])
    executed_queries = executed_queries.replace(expected_suffix, "0" * 6)

    if has_missing_column:
        filename = "initialize_precomputed_lookup_feature_table_missing_column.sql"
    else:
        filename = "initialize_precomputed_lookup_feature_table.sql"

    assert_equal_with_expected_fixture(
        executed_queries,
        f"tests/fixtures/feature_materialize/{filename}",
        update_fixtures,
    )

    if has_missing_column:
        assert "plus_123" not in executed_queries


@pytest.mark.asyncio
async def test_cleanup_error__drop_columns(feature_materialize_service, caplog):
    """
    Test drop_columns error is handled and logged
    """
    with patch(
        "featurebyte.service.feature_materialize.FeatureMaterializeService._get_session",
        side_effect=RuntimeError("Cannot obtain a valid session"),
    ):
        await feature_materialize_service.drop_columns(
            Mock(name="mock_feature_table"), ["a", "b", "c"]
        )
    lines = [record.msg for record in caplog.records if record.msg.startswith("Unexpected")]
    assert len(lines) == 1
    assert lines[0] == "Unexpected error when attempting to modify offline store feature table"


@pytest.mark.asyncio
async def test_cleanup_error__drop_table(feature_materialize_service, caplog):
    """
    Test drop_table error is handled and logged
    """
    with patch(
        "featurebyte.service.feature_materialize.FeatureMaterializeService._get_session",
        side_effect=RuntimeError("Cannot obtain a valid session"),
    ):
        await feature_materialize_service.drop_table(Mock(name="mock_feature_table"))
    lines = [record.msg for record in caplog.records if record.msg.startswith("Unexpected")]
    assert len(lines) == 1
    assert lines[0] == "Unexpected error when attempting to modify offline store feature table"


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features_failure(
    app_container,
    feature_materialize_service,
    offline_store_feature_table,
    feature_materialize_run,
):
    """
    Test FeatureMaterializeRun record updated on failure
    """
    with patch(
        "featurebyte.service.feature_materialize.FeatureMaterializeService._scheduled_materialize_features",
        side_effect=RuntimeError("Fail on purpose"),
    ):
        with pytest.raises(RuntimeError):
            await feature_materialize_service.scheduled_materialize_features(
                offline_store_feature_table, feature_materialize_run.id
            )

    # Check FeatureMaterializeRun record is updated
    feature_materialize_run = await app_container.feature_materialize_run_service.get_document(
        feature_materialize_run.id
    )
    assert feature_materialize_run.feature_materialize_ts is not None
    assert feature_materialize_run.completion_ts is not None
    assert feature_materialize_run.completion_status == "failure"
