"""
Unit tests for BatchFeatureTable class
"""
from typing import Any, Dict

from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte.api.batch_feature_table import BatchFeatureTable
from featurebyte.models.base import CAMEL_CASE_TO_SNAKE_CASE_PATTERN
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


@pytest.fixture(autouse=True)
def mock_online_enable_service_update_data_warehouse():
    """Mock update_data_warehouse method in OnlineEnableService to make it a no-op"""
    with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
        yield


@pytest.fixture(name="batch_feature_table")
def batch_feature_table_fixture(
    deployment, batch_request_table_from_view, snowflake_execute_query_for_materialized_table
):
    """BatchFeatureTable fixture"""
    deployment.enable()
    batch_feature_table = deployment.compute_batch_feature_table(
        batch_request_table_from_view, "my_batch_feature_table"
    )
    return batch_feature_table


class TestBatchFeatureTable(BaseMaterializedTableApiTest[BatchFeatureTable]):
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
        assert (df["shape"] == (500, 1)).all()
