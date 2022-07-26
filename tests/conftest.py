"""
Common fixture for both unit and integration tests
"""
import os
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    """Mask default config path to avoid unintentionally using a real configuration file"""
    with patch.dict(os.environ, {}):
        yield


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature():
    """Fixture for a Feature dict"""
    return {
        "name": "sum_30m",
        "description": None,
        "var_type": "FLOAT",
        "lineage": ("groupby_1", "project_1"),
        "row_index_lineage": ("groupby_1",),
        "graph": {
            "edges": {
                "input_1": ["groupby_1"],
                "groupby_1": ["project_1"],
            },
            "backward_edges": {
                "groupby_1": ["input_1"],
                "project_1": ["groupby_1"],
            },
            "nodes": {
                "groupby_1": {
                    "name": "groupby_1",
                    "output_type": "frame",
                    "parameters": {
                        "agg_func": "sum",
                        "blind_spot": 600,
                        "frequency": 1800,
                        "keys": ["cust_id"],
                        "names": ["sum_30m"],
                        "parent": "col_float",
                        "tile_id": "sum_f1800_m300_b600_3cb3b2b28a359956be02abe635c4446cb50710d7",
                        "time_modulo_frequency": 300,
                        "timestamp": "event_timestamp",
                        "value_by": None,
                        "windows": ["30m"],
                        "serving_names": ["cust_id"],
                    },
                    "type": "groupby",
                },
                "input_1": {
                    "name": "input_1",
                    "output_type": "frame",
                    "parameters": {
                        "columns": [
                            "col_int",
                            "col_float",
                            "col_char",
                            "col_text",
                            "col_binary",
                            "col_boolean",
                            "event_timestamp",
                            "created_at",
                            "cust_id",
                        ],
                        "database_source": {
                            "details": {
                                "account": "sf_account",
                                "database": "sf_database",
                                "sf_schema": "sf_schema",
                                "warehouse": "sf_warehouse",
                            },
                            "type": "snowflake",
                        },
                        "dbtable": {
                            "database_name": "sf_database",
                            "schema_name": "sf_schema",
                            "table_name": "sf_table",
                        },
                        "timestamp": "event_timestamp",
                    },
                    "type": "input",
                },
                "project_1": {
                    "name": "project_1",
                    "output_type": "series",
                    "parameters": {"columns": ["sum_30m"]},
                    "type": "project",
                },
            },
        },
        "node": {
            "name": "project_1",
            "parameters": {"columns": ["sum_30m"]},
            "type": "project",
            "output_type": "series",
        },
        "tabular_source": (
            {
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
                "type": "snowflake",
            },
            {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
        ),
        "readiness": None,
        "version": "V220710",
        "is_default": None,
        "online_enabled": None,
        "event_data_ids": [],
        "created_at": None,
        "parent_id": None,
    }


class Helpers:
    """Helper functions useful in tests"""

    @staticmethod
    def assert_equal_with_expected_fixture(actual, fixture_filename, update_fixture=False):
        """Utility to check that actual is the same as the pre-generated fixture"""
        if update_fixture:
            with open(fixture_filename, "w", encoding="utf-8") as f_handle:
                f_handle.write(actual)
                raise AssertionError(
                    f"Fixture {fixture_filename} updated, please set update_fixture to False"
                )

        with open(fixture_filename, encoding="utf-8") as f_handle:
            expected = f_handle.read()

        assert actual.strip() == expected.strip()


@pytest.fixture(name="helpers")
def helpers_fixture():
    """Fixture for the Helpers class to access helper functions"""
    return Helpers
