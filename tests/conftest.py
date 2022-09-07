"""
Common fixture for both unit and integration tests
"""
from typing import Optional, TextIO

import datetime
import os
import sys
from unittest.mock import patch

import pytest
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
from bson.objectid import ObjectId


class TimestamperTerminalReporter(TerminalReporter):  # pylint: disable=subclassed-final-class
    """
    Add timestamp to the terminal

    References
    ----------
    https://github.com/mbkroese/pytest-timestamper/blob/main/src/pytest_timestamper/plugin.py
    """

    def __init__(self, config: Config, file: Optional[TextIO] = None) -> None:
        super().__init__(config, file)

        # we need a cache to ensure we only print one line per test
        self._cache = {}

    def _locationline(self, nodeid: str, fspath: str, lineno: Optional[int], domain: str):
        key = (nodeid, fspath, lineno, domain)
        if key not in self._cache:
            baseline = super()._locationline(nodeid, fspath, lineno, domain)
            formatted_datetime = datetime.datetime.now().strftime(self.config.option.datefmt)
            formatted_prefix = self.config.option.prefixfmt.format(
                formatted_datetime=formatted_datetime
            )
            self._cache[key] = formatted_prefix + baseline
        return self._cache[key]


def pytest_configure(config):
    """Set up additional pytest markers"""
    # register an additional marker
    config.addinivalue_line("markers", "no_mock_process_store: mark test to not mock process store")

    if config.pluginmanager.has_plugin("terminalreporter"):
        reporter = config.pluginmanager.get_plugin("terminalreporter")
        config.pluginmanager.unregister(reporter, "terminalreporter")
        config.pluginmanager.register(
            TimestamperTerminalReporter(config, sys.stdout), "terminalreporter"
        )


def pytest_addoption(parser):
    """Set up additional pytest options"""
    parser.addoption("--update-fixtures", action="store_true", default=False)


@pytest.fixture(scope="session")
def update_fixtures(pytestconfig):
    """Fixture corresponding to pytest --update-fixtures option"""
    return pytestconfig.getoption("update_fixtures")


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
        "dtype": "FLOAT",
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
                        "tile_id": "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
                        "aggregation_id": "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
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
                        "feature_store": {
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
        "readiness": "DRAFT",
        "version": "V220710",
        "online_enabled": None,
        "event_data_ids": [ObjectId()],
        "entity_ids": [ObjectId()],
        "created_at": None,
        "updated_at": None,
        "user_id": None,
    }
