"""
Unit tests for BatchFeatureTable class
"""
from unittest.mock import patch

import pandas as pd
import pytest

from featurebyte import BatchFeatureTable


@pytest.fixture(autouse=True)
def mock_online_enable_service_update_data_warehouse():
    """Mock update_data_warehouse method in OnlineEnableService to make it a no-op"""
    with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
        yield


@pytest.fixture(name="batch_feature_table")
def batch_feature_table_fixture(deployment, batch_request_table_from_view):
    """BatchFeatureTable fixture"""
    deployment.enable()
    batch_feature_table = deployment.get_batch_features(
        batch_request_table_from_view, "my_batch_feature_table"
    )
    return batch_feature_table


def test_get(batch_feature_table):
    """Test get method"""
    retrieved_batch_feature_table = BatchFeatureTable.get(batch_feature_table.name)
    assert retrieved_batch_feature_table == batch_feature_table


def test_list(batch_feature_table):
    """Test list method"""
    df = BatchFeatureTable.list()
    expected = pd.DataFrame(
        [
            {
                "id": batch_feature_table.id,
                "name": "my_batch_feature_table",
                "feature_store_name": "sf_featurestore",
                "batch_request_table_name": "batch_request_table_from_event_view",
                "created_at": batch_feature_table.created_at,
            }
        ]
    )
    pd.testing.assert_frame_equal(df, expected)


def test_info(batch_feature_table):
    """Test info method"""
    info_dict = batch_feature_table.info()
    assert info_dict["deployment_name"].startswith(
        'Deployment (feature_list: "my_feature_list", version: V'
    )
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
        "columns_info": [],
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }
