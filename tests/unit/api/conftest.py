"""
Common test fixtures used across api test directories
"""
import textwrap
from unittest.mock import patch

import pytest


@pytest.fixture(name="mock_get_persistent")
def mock_get_persistent_function(git_persistent):
    """
    Mock GitDB in featurebyte.app
    """
    with patch("featurebyte.app._get_persistent") as mock_persistent:
        persistent, _ = git_persistent
        mock_persistent.return_value = persistent
        yield mock_persistent


@pytest.fixture(autouse=True)
def mock_settings_env_vars(mock_config_path_env, mock_get_persistent):
    """Use these fixtures for all API tests"""
    _ = mock_config_path_env, mock_get_persistent
    yield


@pytest.fixture()
def expected_snowflake_table_preview_query() -> str:
    """
    Expected preview_sql output
    """
    return textwrap.dedent(
        """
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "event_timestamp" AS "event_timestamp",
          "created_at" AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
        """
    ).strip()
