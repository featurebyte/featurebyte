"""
Unit tests for BatchFeatureTable class
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd
import pytest

from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.models.base import CAMEL_CASE_TO_SNAKE_CASE_PATTERN
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(name="batch_feature_table")
def batch_feature_table_fixture(
    deployment,
    batch_request_table_from_view,
    mock_deployment_flow,
):
    """BatchFeatureTable fixture"""
    deployment.enable()
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table_from_view, "my_batch_feature_table"
    )
    return batch_feature_table


class TestBatchFeatureTable(BaseMaterializedTableApiTest):
    """
    Test batch feature table
    """

    table_type = BatchFeatureTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["deployment_name"].startswith("Deployment with my_feature_list_V")
        assert info_dict["table_details"]["table_name"].startswith("BATCH_FEATURE_TABLE_")
        assert info_dict == {
            "name": "my_batch_feature_table",
            "deployment_name": info_dict["deployment_name"],
            "batch_request_table_name": "batch_request_table_from_event_view",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    def assert_list_df(self, df: pd.DataFrame) -> None:
        assert df.columns.tolist() == [
            "id",
            "name",
            "feature_store_name",
            "batch_request_table_name",
            "shape",
            "created_at",
        ]
        expected_name = CAMEL_CASE_TO_SNAKE_CASE_PATTERN.sub(r"_\1", self.table_type_name).lower()
        assert df["name"].tolist() == [f"my_{expected_name}"]
        assert df["feature_store_name"].tolist() == ["sf_featurestore"]
        assert df["batch_request_table_name"].tolist() == ["batch_request_table_from_event_view"]
        assert df["shape"].tolist() == [[500, 3]]


class TestBatchFeatureTableFromView(BaseMaterializedTableApiTest):
    """
    Test batch feature table from view
    """

    table_type = BatchFeatureTable

    @pytest.fixture(name="table_under_test")
    def get_table_under_test_fixture(
        self,
        deployment,
        snowflake_event_view,
        snowflake_execute_query_for_materialized_table,
        mock_deployment_flow,
    ):
        """BatchFeatureTable fixture"""
        deployment.enable()
        batch_feature_table = deployment.compute_batch_feature_table(
            snowflake_event_view, "my_batch_feature_table_from_view"
        )
        return batch_feature_table

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["deployment_name"].startswith("Deployment with my_feature_list_V")
        assert info_dict["table_details"]["table_name"].startswith("BATCH_FEATURE_TABLE_")
        assert info_dict == {
            "name": "my_batch_feature_table_from_view",
            "deployment_name": info_dict["deployment_name"],
            "batch_request_table_name": None,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    def assert_list_df(self, df: pd.DataFrame) -> None:
        assert df.columns.tolist() == [
            "id",
            "name",
            "feature_store_name",
            "batch_request_table_name",
            "shape",
            "created_at",
        ]
        assert df["name"].tolist() == ["my_batch_feature_table_from_view"]
        assert df["feature_store_name"].tolist() == ["sf_featurestore"]
        assert df["batch_request_table_name"].tolist() == [None]
        assert df["shape"].tolist() == [[500, 3]]


def test_batch_feature_table_with_point_in_time(
    deployment,
    batch_request_table_from_view,
    mock_deployment_flow,
    snowflake_execute_query_for_materialized_table,
    update_fixtures,
):
    """
    Test creating batch feature table with point in time
    """
    deployment.enable()
    _ = deployment.compute_batch_feature_table(
        batch_request_table_from_view,
        "my_batch_feature_table_with_point_in_time",
        point_in_time=datetime(2023, 1, 1, 10, 0, 0),
    )
    query = None
    for execute_query_call_args in snowflake_execute_query_for_materialized_table.call_args_list:
        args, kwargs = execute_query_call_args
        if args:
            query = args[0]
        else:
            query = kwargs.get("query")
        if query is not None and "ONLINE_REQUEST_TABLE" in query:
            break
    else:
        pytest.fail("Expected query for ONLINE_REQUEST_TABLE not found")
    assert_equal_with_expected_fixture(
        query,
        "tests/fixtures/api/test_batch_feature_table/query_with_point_in_time.sql",
        update_fixtures,
    )
