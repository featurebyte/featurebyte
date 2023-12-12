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

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.online_store import OnlineFeatureSpec
from tests.util.helper import assert_equal_with_expected_fixture


async def create_online_store_compute_query(online_store_compute_query_service, feature_model):
    """
    Helper to create online store compute query since that step is skipped because of
    mock_update_data_warehouse
    """
    extended_feature_model = ExtendedFeatureModel(**feature_model.dict(by_alias=True))
    online_feature_spec = OnlineFeatureSpec(feature=extended_feature_model)
    for query in online_feature_spec.precompute_queries:
        await online_store_compute_query_service.create_document(query)


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
    mock_update_data_warehouse,
):
    """
    Fixture for FeatureMaterializeService
    """
    _ = mock_update_data_warehouse

    deployment_id = ObjectId()
    await app_container.deploy_service.create_deployment(
        feature_list_id=production_ready_feature_list.id,
        deployment_id=deployment_id,
        deployment_name=None,
        to_enable_deployment=True,
    )
    for feature_id in production_ready_feature_list.feature_ids:
        feature_model = await app_container.feature_service.get_document(
            document_id=feature_id,
        )
        await create_online_store_compute_query(
            app_container.online_store_compute_query_service, feature_model
        )
        with patch(
            "featurebyte.service.offline_store_feature_table_manager.FeatureMaterializeService.initialize_new_columns"
        ):
            await app_container.offline_store_feature_table_manager_service.handle_online_enabled_feature(
                feature_model,
            )
    deployment = await app_container.deployment_service.get_document(document_id=deployment_id)
    deployed_feature_list = await app_container.feature_list_service.get_document(
        document_id=deployment.feature_list_id
    )
    yield deployed_feature_list


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


def extract_session_executed_queries(mock_snowflake_session, func="execute_query_long_running"):
    """
    Helper to extract executed queries from mock_snowflake_session
    """
    assert func in {"execute_query_long_running", "execute_query"}
    queries = []
    for call_obj in getattr(mock_snowflake_session, func).call_args_list:
        args, _ = call_obj
        queries.append(args[0] + ";")
    return "\n\n".join(queries)


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
        "column_names": ["sum_30m"],
        "data_types": ["FLOAT"],
        "feature_timestamp": datetime(2022, 1, 1, 0, 0),
        "serving_names": ["cust_id"],
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


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features(
    app_container,
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
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

    # Check online materialization called
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "fb_entity_cust_id_fjs_1800_300_600_ttl"
    assert {"columns": ["sum_30m"], "start_date": None, "end_date": datetime(2022, 1, 1, 0, 0)}

    # Check last materialization timestamp updated
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    assert updated_feature_table.last_materialized_at == datetime(2022, 1, 1, 0, 0)


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_scheduled_materialize_features_if_materialized_before(
    app_container,
    feature_materialize_service,
    offline_store_feature_table,
    mock_materialize_partial,
):
    """
    Test calling scheduled_materialize_features when the feature table has already been materialized
    before. When calling online materialize it should provide the feature table's
    last_materialized_at as start date.
    """
    offline_store_feature_table.last_materialized_at = datetime(2022, 1, 1, 0, 0)

    with freeze_time("2022-01-02 00:00:00"):
        await feature_materialize_service.scheduled_materialize_features(
            offline_store_feature_table
        )

    # Check online materialization
    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "fb_entity_cust_id_fjs_1800_300_600_ttl"
    assert {
        "columns": ["sum_30m"],
        "start_date": datetime(2022, 1, 1, 0, 0),
        "end_date": datetime(2022, 1, 2, 0, 0),
    }

    # Check last materialization timestamp updated
    updated_feature_table = await app_container.offline_store_feature_table_service.get_document(
        offline_store_feature_table.id
    )
    assert updated_feature_table.last_materialized_at == datetime(2022, 1, 2, 0, 0)


@pytest.mark.usefixtures("mock_get_feature_store_session")
@pytest.mark.asyncio
async def test_initialize_new_columns__table_does_not_exist(
    feature_materialize_service,
    mock_snowflake_session,
    offline_store_feature_table,
    mock_materialize_partial,
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

    _, kwargs = mock_materialize_partial.call_args
    _ = kwargs.pop("feature_store")
    feature_view = kwargs.pop("feature_view")
    assert feature_view.name == "fb_entity_cust_id_fjs_1800_300_600_ttl"
    assert {"columns": ["sum_30m"], "start_date": None, "end_date": datetime(2022, 1, 1, 0, 0)}


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
    assert feature_view.name == "fb_entity_cust_id_fjs_1800_300_600_ttl"
    assert {"columns": ["sum_30m"], "start_date": None, "end_date": datetime(2022, 1, 1, 0, 0)}