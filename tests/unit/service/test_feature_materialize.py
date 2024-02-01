"""
Test FeatureMaterializeService
"""
from dataclasses import asdict
from datetime import datetime
from unittest.mock import call, patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from freezegun import freeze_time

from featurebyte.common.model_util import get_version
from featurebyte.models.offline_store_feature_table import OnlineStoreLastMaterializedAt
from featurebyte.schema.catalog import CatalogOnlineStoreUpdate
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    deploy_feature_list,
    extract_session_executed_queries,
)


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(enable_feast_integration):
    """
    Enable feast integration for all tests in this module
    """
    _ = enable_feast_integration


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


@pytest.fixture(name="is_online_store_registered_for_catalog")
def is_online_store_registered_for_catalog_fixture():
    """
    Fixture to determine if catalog is configured with an online store
    """
    return True


@pytest_asyncio.fixture(name="deployed_feature_list")
async def deployed_feature_list_fixture(
    app_container,
    production_ready_feature_list,
    online_store,
    mock_update_data_warehouse,
    is_online_store_registered_for_catalog,
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

    # TODO: use deploy_feature() helper
    deployment_id = ObjectId()
    with patch(
        "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService.initialize_new_columns"
    ):
        await app_container.deploy_service.create_deployment(
            feature_list_id=production_ready_feature_list.id,
            deployment_id=deployment_id,
            deployment_name=None,
            to_enable_deployment=True,
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
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for a feature list with composite entity
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    float_feature_composite_entity.save()
    feature_list_model = await deploy_feature_list(
        app_container, "my_list", [float_feature_composite_entity.id]
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
    feature_list_model = await deploy_feature_list(
        app_container, "my_list", [feature_without_entity.id]
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
    with patch("featurebyte.service.feature_materialize.ObjectId") as patched_object_id:
        patched_object_id.return_value = ObjectId("000000000000000000000000")
        yield patched_object_id


@pytest.fixture(name="freeze_feature_timestamp", autouse=True)
def freeze_feature_timestamp_fixture():
    """
    Patch ObjectId to return a fixed value so that queries are deterministic
    """
    with freeze_time("2022-01-01 00:00:00"):
        yield


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
    ) as materialized_features:
        pass

    assert len(mock_snowflake_session.execute_query_long_running.call_args_list) == 2
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
@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    is_online_store_registered_for_catalog,
    update_fixtures,
):
    """
    Test scheduled_materialize_features
    """
    await feature_materialize_service.scheduled_materialize_features(offline_store_feature_table)

    executed_queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
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
    assert updated_feature_table.last_materialized_at == datetime(2022, 1, 1, 0, 0)

    # Check online last materialization timestamp updated
    if is_online_store_registered_for_catalog:
        assert len(updated_feature_table.online_stores_last_materialized_at) == 1
        assert updated_feature_table.online_stores_last_materialized_at[0].value == datetime(
            2022, 1, 1, 0, 0
        )
    else:
        assert len(updated_feature_table.online_stores_last_materialized_at) == 0


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features_if_materialized_before(
    app_container,
    feature_materialize_service,
    offline_store_feature_table,
    online_store,
    mock_materialize_partial,
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

    with freeze_time("2022-01-02 00:00:00"):
        await feature_materialize_service.scheduled_materialize_features(
            offline_store_feature_table
        )

    # Check online materialization
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "cat1_cust_id_30m"
    assert kwargs == {
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
):
    """
    Test initialize_new_columns when feature table is not yet created
    """

    def mock_execute_query(query):
        if "LIMIT 1" in query:
            raise ValueError()

    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
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
):
    """
    Test initialize_new_columns when feature table already exists
    """

    async def mock_list_table_schema(*args, **kwargs):
        _ = args
        _ = kwargs
        return {"some_existing_col": "some_info"}

    async def mock_execute_query(query):
        if 'MAX("__feature_timestamp")' in query:
            return pd.DataFrame([{"RESULT": "2022-10-15 10:00:00"}])

    mock_snowflake_session.list_table_schema.side_effect = mock_list_table_schema
    mock_snowflake_session.execute_query.side_effect = mock_execute_query

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
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
        "columns": [f"sum_30m_{get_version()}"],
        "end_date": datetime(2022, 10, 15, 10, 0, 0),
        "start_date": None,
        "with_feature_timestamp": True,
    }


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__databricks_unity(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
    update_fixtures,
):
    """
    Test initialize_new_columns when session is databricks_unity
    """

    def mock_execute_query(query):
        if "LIMIT 1" in query:
            raise ValueError()

    mock_snowflake_session.source_type = "databricks_unity"
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    await feature_materialize_service.initialize_new_columns(offline_store_feature_table)
    queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
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
    queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
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


@pytest.mark.asyncio
async def test_materialize_features_composite_entity(
    offline_store_feature_table_composite_entity,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test materialize_features for a feature table with composite entity
    """
    _ = mock_get_feature_store_session

    async with feature_materialize_service.materialize_features(
        feature_table_model=offline_store_feature_table_composite_entity,
    ) as materialized_features:
        pass

    # Check concatenated column generated
    assert materialized_features.serving_names_and_column_names == [
        "cust_id",
        "another_key",
        "cust_id x another_key",
        f"composite_entity_feature_1d_{get_version()}",
    ]

    # Check that executed queries are correct
    executed_queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        executed_queries,
        "tests/fixtures/feature_materialize/materialize_features_queries_composite_entity.sql",
        update_fixtures,
    )


@pytest.mark.asyncio
async def test_materialize_features_no_entity_databricks_unity(
    offline_store_feature_table_no_entity,
    feature_materialize_service,
    mock_get_feature_store_session,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test creating a feature table with no entity sets the primary constraint on dummy column
    """
    _ = mock_get_feature_store_session

    def mock_execute_query(query):
        if "LIMIT 1" in query:
            raise ValueError()

    mock_snowflake_session.source_type = "databricks_unity"
    mock_snowflake_session.execute_query.side_effect = mock_execute_query
    mock_snowflake_session._no_schema_error = ValueError

    patch.stopall()  # stop the patcher on initialize_new_columns()
    await feature_materialize_service.initialize_new_columns(offline_store_feature_table_no_entity)

    queries = extract_session_executed_queries(mock_snowflake_session, "execute_query")
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/feature_materialize/initialize_features_no_entity_databricks_unity.sql",
        update_fixtures,
    )
