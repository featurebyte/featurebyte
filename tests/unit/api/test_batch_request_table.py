"""
Unit tests for BatchRequestTable class
"""
from typing import Any, Dict

import pandas as pd
import pytest

from featurebyte.api.batch_request_table import BatchRequestTable
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestBatchRequestTable(BaseMaterializedTableApiTest[BatchRequestTable]):
    """
    Test batch request table
    """

    table_type = BatchRequestTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("BATCH_REQUEST_TABLE_")
        assert info_dict == {
            "name": "batch_request_table_from_source_table",
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
        }

    @pytest.mark.skip(reason="use other test due to testing of more fixtures")
    def test_list(self, table_under_test):
        ...


def test_list(batch_request_table_from_source, batch_request_table_from_view):
    """
    Test listing BatchRequestTable objects
    """
    df = BatchRequestTable.list()
    df = df.sort_values("name").reset_index(drop=True)
    expected = pd.DataFrame(
        [
            {
                "id": batch_request_table_from_view.id,
                "name": "batch_request_table_from_event_view",
                "type": "view",
                "shape": (500, 1),
                "feature_store_name": "sf_featurestore",
                "created_at": batch_request_table_from_view.created_at,
            },
            {
                "id": batch_request_table_from_source.id,
                "name": "batch_request_table_from_source_table",
                "type": "source_table",
                "shape": (500, 1),
                "feature_store_name": "sf_featurestore",
                "created_at": batch_request_table_from_source.created_at,
            },
        ]
    )
    pd.testing.assert_frame_equal(df, expected)
