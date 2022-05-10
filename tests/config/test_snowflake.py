"""
Unit Test Cases for SnowflakeConfig
"""

from unittest import mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.config import SnowflakeConfig


@pytest.fixture(name="test_snowflake")
@mock.patch("featurebyte.config.snowflake.connector.connect")
def snowflake(mock_connect):
    """
    Pytest Fixture for SnowflakeConfig instance.

    Args:
        mock_connect: mocked Snowflake connection object.

    Returns:
        f_snowflake: Fixture for SnowflakeConfig instance.
    """
    mock_connect.size_effect = None
    f_snowflake = SnowflakeConfig(None, None, None, "database", "schema", "compute_wh", "table")
    return f_snowflake


@mock.patch("featurebyte.config.snowflake.SnowflakeConfig._execute_df")
def test_get_table_metadata(mock_df, test_snowflake):
    """
    Test Case for get_table_metadata.

    Args:
        mock_df: mock object for _execute_df function.
        test_snowflake: Fixture for SnowflakeConfig instance.

    Returns:

    """
    mock_df.return_value = pd.DataFrame(
        {"COLUMN_NAME": ["col1", "col2"], "DATA_TYPE": ["TEXT", "NUMBER"]}
    )
    dataframe = test_snowflake.get_table_metadata()

    sql = (
        " select COLUMN_NAME, DATA_TYPE from table(%s) "
        " where table_name = %s and table_schema = %s "
    )
    mock_df.assert_called_with(sql, ("database.information_schema.columns", "TABLE", "SCHEMA"))
    assert len(dataframe) == 2
    expected_df = pd.DataFrame(
        {
            "COLUMN_NAME": ["col1", "col2"],
            "DATA_TYPE": ["TEXT", "NUMBER"],
            "VARIABLE_TYPE": ["string", "numeric"],
        }
    )
    assert_frame_equal(dataframe, expected_df)


@mock.patch("featurebyte.config.snowflake.SnowflakeConfig._execute_df")
def test_get_table_sample_data(mock_df, test_snowflake):
    """
    Test Case for get_table_sample_data.

    Args:
        mock_df: mock object for _execute_df function.
        test_snowflake: Fixture for SnowflakeConfig instance.

    Returns:

    """
    return_df = pd.DataFrame({"id": ["id1", "id2", "id3"], "name": ["name1", "name2", "name3"]})
    mock_df.return_value = return_df
    dataframe = test_snowflake.get_table_sample_data()

    mock_df.assert_called_with("select * from table(%s) limit %s", ("table", 10))
    assert len(dataframe) == 3
    assert len(dataframe.columns) == 2
    assert_frame_equal(dataframe, return_df)
