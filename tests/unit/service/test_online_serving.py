"""
Tests for OnlineServingService
"""

import json
import os
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.exception import DeploymentNotEnabledError, RequiredEntityNotProvidedError
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    deploy_feature_ids,
    extract_session_executed_queries,
)


@pytest_asyncio.fixture
async def deployed_feature_list_multiple_features(
    app_container,
    float_feature,
    feature_without_entity,
    mock_update_data_warehouse,
    mock_offline_store_feature_manager_dependencies,
):
    """
    Fixture for deployed features
    """
    _ = mock_update_data_warehouse
    _ = mock_offline_store_feature_manager_dependencies

    float_feature.save()
    feature_without_entity.save()
    feature_list_model = await deploy_feature_ids(
        app_container, "my_list", [float_feature.id, feature_without_entity.id]
    )
    return feature_list_model


@pytest.fixture
def entity_serving_names():
    """
    Fixture for entity serving names for requesting online features
    """
    return [{"cust_id": 1}]


@pytest.mark.asyncio
async def test_feature_list_not_deployed(
    online_serving_service,
    feature_list,
    entity_serving_names,
):
    """
    Test getting online features for not yet deployed feature list is not allowed
    """
    with pytest.raises(DeploymentNotEnabledError) as exc:
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=feature_list,
            request_data=entity_serving_names,
        )
    assert str(exc.value) == "Deployment is not enabled"


@pytest.mark.asyncio
async def test_missing_entity_error(online_serving_service, deployed_feature_list):
    """
    Test requesting online features when an required entity is not provided
    """
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            request_data=[{"wrong_entity": 123}],
        )
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.fixture(name="mock_session_for_online_serving")
def mock_session_for_online_serving_fixture(mock_snowflake_session):
    """Mock session for online serving"""

    async def mock_execute_query(query):
        _ = query
        if "is_row_index_valid" in query:
            return pd.DataFrame({"is_row_index_valid": [True]})
        return pd.DataFrame({"cust_id": [1], "feature_value": [123.0], "__FB_TABLE_ROW_INDEX": [0]})

    with patch(
        "featurebyte.service.online_serving.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_snowflake_session.execute_query = Mock(side_effect=mock_execute_query)
        mock_snowflake_session.execute_query_long_running = Mock(side_effect=mock_execute_query)
        mock_snowflake_session.generate_session_unique_id.return_value = "1"
        mock_get_feature_store_session.return_value = mock_snowflake_session
        yield mock_snowflake_session


@pytest.fixture
def patched_num_features_per_query():
    """
    Patch the NUM_FEATURES_PER_QUERY parameter to trigger executing feature query in batches
    """
    with patch("featurebyte.session.session_helper.NUM_FEATURES_PER_QUERY", 1):
        yield


@pytest.mark.asyncio
async def test_feature_list_deployed(
    online_serving_service,
    deployed_feature_list,
    entity_serving_names,
    mock_session_for_online_serving,
    update_fixtures,
):
    """
    Test getting online features request for a valid feature list
    """
    result = await online_serving_service.get_online_features_from_feature_list(
        feature_list=deployed_feature_list,
        request_data=entity_serving_names,
    )

    # Check result
    assert result.model_dump() == {"features": [{"cust_id": 1.0, "feature_value": 123.0}]}

    # Check query used
    queries = extract_session_executed_queries(mock_session_for_online_serving)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_get_online_features.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.asyncio
async def test_feature_list_deployed_with_output_table(
    online_serving_service,
    deployed_feature_list,
    entity_serving_names,
    mock_session_for_online_serving,
    update_fixtures,
):
    """
    Test getting online features request with output table
    """
    await online_serving_service.get_online_features_from_feature_list(
        feature_list=deployed_feature_list,
        request_data=entity_serving_names,
        output_table_details=TableDetails(
            database_name="output_db_name",
            schema_name="output_schema_name",
            table_name="output_table_name",
        ),
    )

    queries = extract_session_executed_queries(mock_session_for_online_serving)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_get_online_features_with_output_table.sql",
        update_fixture=update_fixtures,
    )


@pytest_asyncio.fixture(name="batch_request_table")
async def batch_request_table_fixture(app_container, test_dir):
    """Batch request table fixture"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/batch_request_table.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        document = await app_container.batch_request_table_service.create_document(
            data=BatchRequestTableModel(
                **payload,
                location=TabularSource(
                    feature_store_id=ObjectId("5f9f1b5b0b1b9c0b5c1b1fff"),
                    table_details=TableDetails(
                        database_name="req_db_name",
                        schema_name="req_schema_name",
                        table_name="req_table_name",
                    ),
                ),
                columns_info=[{"name": "cust_id", "dtype": "INT"}],
                num_rows=500,
            )
        )
        return document


@pytest.mark.asyncio
async def test_feature_list_deployed_with_batch_request_table(
    online_serving_service,
    deployed_feature_list,
    mock_session_for_online_serving,
    batch_request_table,
    update_fixtures,
):
    """
    Test getting online features request with batch request table
    """
    await online_serving_service.get_online_features_from_feature_list(
        feature_list=deployed_feature_list,
        request_data=batch_request_table,
        output_table_details=TableDetails(
            database_name="some_database", schema_name="some_schema", table_name="some_table"
        ),
    )

    queries = extract_session_executed_queries(mock_session_for_online_serving)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_get_online_features_with_batch_request_table.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.usefixtures("patched_num_features_per_query")
@pytest.mark.asyncio
async def test_get_online_features_multiple_queries__dataframe(
    online_serving_service,
    deployed_feature_list_multiple_features,
    entity_serving_names,
    mock_session_for_online_serving,
    update_fixtures,
):
    """
    Test getting online features with multiple queries (dataframe input)
    """
    await online_serving_service.get_online_features_from_feature_list(
        feature_list=deployed_feature_list_multiple_features,
        request_data=entity_serving_names,
    )

    # Check queries used
    queries = extract_session_executed_queries(mock_session_for_online_serving)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_get_online_features_multiple_queries_dataframe.sql",
        update_fixture=update_fixtures,
    )

    # REQUEST_TABLE_1 is an intermediate table that should be dropped
    assert mock_session_for_online_serving.drop_table.call_args_list == [
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
            timeout=86400,
        ),
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_1",
            if_exists=True,
            timeout=86400,
        ),
        call(table_name="REQUEST_TABLE_1", schema_name="sf_schema", database_name="sf_db"),
    ]


@pytest.mark.usefixtures("patched_num_features_per_query")
@pytest.mark.asyncio
async def test_get_online_features_multiple_queries__batch_request_table(
    online_serving_service,
    deployed_feature_list_multiple_features,
    mock_session_for_online_serving,
    batch_request_table,
    update_fixtures,
):
    """
    Test getting online features request with batch request table (batch request table input)
    """
    await online_serving_service.get_online_features_from_feature_list(
        feature_list=deployed_feature_list_multiple_features,
        request_data=batch_request_table,
        output_table_details=TableDetails(
            database_name="some_database", schema_name="some_schema", table_name="some_table"
        ),
    )

    # Check queries used
    queries = extract_session_executed_queries(mock_session_for_online_serving)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/expected_get_online_features_multiple_queries_batch_request_table.sql",
        update_fixture=update_fixtures,
    )

    # Only two intermediate tables in this case. Request table is already a materialized table, so
    # it should not be dropped.
    assert mock_session_for_online_serving.drop_table.call_args_list == [
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_0",
            if_exists=True,
            timeout=86400,
        ),
        call(
            database_name="sf_db",
            schema_name="sf_schema",
            table_name="__TEMP_000000000000000000000000_1",
            if_exists=True,
            timeout=86400,
        ),
    ]
