"""
Unit tests for TargetTable class
"""
from typing import Any, Dict

from featurebyte.api.target_table import TargetTable
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestTargetTable(BaseMaterializedTableApiTest[TargetTable]):
    """
    Test target table
    """

    table_type = TargetTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("TARGET_TABLE_")
        assert info_dict == {
            "name": "my_target_table",
            "target_name": "float_target",
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
