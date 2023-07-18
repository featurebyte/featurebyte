"""
Unit tests for HistoricalFeatureTable class
"""
from typing import Any, Dict

from featurebyte.api.historical_feature_table import HistoricalFeatureTable
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestHistoricalFeatureTable(BaseMaterializedTableApiTest[HistoricalFeatureTable]):
    """
    Test historical feature table
    """

    table_type = HistoricalFeatureTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("HISTORICAL_FEATURE_TABLE_")
        assert isinstance(info_dict["feature_list_version"], str)
        assert info_dict == {
            "name": "my_historical_feature_table",
            "feature_list_name": "feature_list_for_historical_feature_table",
            "feature_list_version": info_dict["feature_list_version"],
            "observation_table_name": "observation_table_from_source_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
        }
