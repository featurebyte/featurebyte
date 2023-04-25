"""
Tests for OnlineServingService
"""
import json
import os
import textwrap
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import SourceType
from featurebyte.exception import FeatureListNotOnlineEnabledError, RequiredEntityNotProvidedError
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails


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
    with pytest.raises(FeatureListNotOnlineEnabledError) as exc:
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=feature_list,
            request_data=entity_serving_names,
            get_credential=Mock(),
        )
    assert str(exc.value) == "Feature List is not online enabled"


@pytest.mark.asyncio
async def test_missing_entity_error(online_serving_service, deployed_feature_list):
    """
    Test requesting online features when an required entity is not provided
    """
    with pytest.raises(RequiredEntityNotProvidedError) as exc:
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            request_data=[{"wrong_entity": 123}],
            get_credential=Mock(),
        )
    expected = (
        'Required entities are not provided in the request: customer (serving name: "cust_id")'
    )
    assert str(exc.value) == expected


@pytest.fixture(name="mock_session_for_online_serving")
def mock_session_for_online_serving_fixture():
    """Mock session for online serving"""

    async def mock_execute_query(query):
        _ = query
        return pd.DataFrame({"cust_id": [1], "feature_value": [123.0]})

    return Mock(
        name="mock_session_for_online_serving",
        execute_query=Mock(side_effect=mock_execute_query),
        execute_query_long_running=Mock(side_effect=mock_execute_query),
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.fixture(name="expected_online_feature_query")
def expected_online_feature_query_fixture():
    """Expected query for online feature"""
    return textwrap.dedent(
        """
        WITH ONLINE_REQUEST_TABLE AS (
          SELECT
            REQ."cust_id",
            SYSDATE() AS POINT_IN_TIME
          FROM (
            SELECT
              1 AS "cust_id"
          ) AS REQ
        ), _FB_AGGREGATED AS (
          SELECT
            REQ."cust_id",
            REQ."POINT_IN_TIME",
            "T0"."_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
          FROM ONLINE_REQUEST_TABLE AS REQ
          LEFT JOIN (
            SELECT
              "cust_id" AS "cust_id",
              "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
            FROM online_store_ff698d3d3703c3afda95ec949ba386a02c6bd61d
          ) AS T0
            ON REQ."cust_id" = T0."cust_id"
        )
        SELECT
          AGG."cust_id",
          "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "sum_30m"
        FROM _FB_AGGREGATED AS AGG
        """
    ).strip()


@pytest.mark.asyncio
async def test_feature_list_deployed(
    online_serving_service,
    deployed_feature_list,
    entity_serving_names,
    mock_session_for_online_serving,
    expected_online_feature_query,
):
    """
    Test getting online features request for a valid feature list
    """
    with patch(
        "featurebyte.service.online_serving.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = mock_session_for_online_serving
        result = await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            request_data=entity_serving_names,
            get_credential=Mock(),
        )

    # Check result
    assert result.dict() == {"features": [{"cust_id": 1.0, "feature_value": 123.0}]}

    # Check query used
    assert len(mock_session_for_online_serving.execute_query.call_args_list) == 1
    args, _ = mock_session_for_online_serving.execute_query.call_args
    assert args[0] == expected_online_feature_query


@pytest.mark.asyncio
async def test_feature_list_deployed_with_output_table(
    online_serving_service,
    deployed_feature_list,
    entity_serving_names,
    mock_session_for_online_serving,
    expected_online_feature_query,
):
    """
    Test getting online features request with output table
    """
    with patch(
        "featurebyte.service.online_serving.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = mock_session_for_online_serving
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            request_data=entity_serving_names,
            get_credential=Mock(),
            output_table_details=TableDetails(
                database_name="output_db_name",
                schema_name="output_schema_name",
                table_name="output_table_name",
            ),
        )

    assert len(mock_session_for_online_serving.execute_query_long_running.call_args_list) == 1
    args, _ = mock_session_for_online_serving.execute_query_long_running.call_args
    expected_with_output_table = (
        'CREATE TABLE "output_db_name"."output_schema_name"."output_table_name" AS\n'
        + expected_online_feature_query
    )
    assert args[0] == expected_with_output_table


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
            )
        )
        return document


@pytest.mark.asyncio
async def test_feature_list_deployed_with_batch_request_table(
    online_serving_service,
    deployed_feature_list,
    mock_session_for_online_serving,
    expected_online_feature_query,
    batch_request_table,
):
    """
    Test getting online features request with batch request table
    """
    with patch(
        "featurebyte.service.online_serving.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = mock_session_for_online_serving
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            request_data=batch_request_table,
            get_credential=Mock(),
        )

    assert len(mock_session_for_online_serving.execute_query.call_args_list) == 1
    args, _ = mock_session_for_online_serving.execute_query.call_args
    assert (
        args[0]
        == textwrap.dedent(
            """
        WITH ONLINE_REQUEST_TABLE AS (
          SELECT
            REQ."cust_id",
            SYSDATE() AS POINT_IN_TIME
          FROM (
            SELECT
              *
            FROM "req_db_name"."req_schema_name"."req_table_name"
          ) AS REQ
        ), _FB_AGGREGATED AS (
          SELECT
            REQ."cust_id",
            REQ."POINT_IN_TIME",
            "T0"."_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
          FROM ONLINE_REQUEST_TABLE AS REQ
          LEFT JOIN (
            SELECT
              "cust_id" AS "cust_id",
              "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
            FROM online_store_ff698d3d3703c3afda95ec949ba386a02c6bd61d
          ) AS T0
            ON REQ."cust_id" = T0."cust_id"
        )
        SELECT
          AGG."cust_id",
          "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "sum_30m"
        FROM _FB_AGGREGATED AS AGG
        """
        ).strip()
    )
