"""
Unit test for DatabaseTable
"""
import pandas as pd

from featurebyte.enum import DBVarType, TableDataType
from tests.util.helper import check_sdk_code_generation


def test_database_table(snowflake_database_table, expected_snowflake_table_preview_query):
    """
    Test DatabaseTable preview functionality
    """
    assert snowflake_database_table.preview_sql() == expected_snowflake_table_preview_query
    expected_dtypes = pd.Series(
        {
            "col_int": DBVarType.INT,
            "col_float": DBVarType.FLOAT,
            "col_char": DBVarType.CHAR,
            "col_text": DBVarType.VARCHAR,
            "col_binary": DBVarType.BINARY,
            "col_boolean": DBVarType.BOOL,
            "event_timestamp": DBVarType.TIMESTAMP_TZ,
            "created_at": DBVarType.TIMESTAMP_TZ,
            "cust_id": DBVarType.INT,
        }
    )
    pd.testing.assert_series_equal(snowflake_database_table.dtypes, expected_dtypes)


def test_database_table_node_parameters(snowflake_database_table):
    """Test database table node parameters"""
    node_params = snowflake_database_table.frame.node.parameters
    assert node_params.type == TableDataType.GENERIC


def test_database_table_get_input_node(snowflake_database_table):
    """Test database table get input node"""
    pruned_graph, mapped_node = snowflake_database_table.frame.extract_pruned_graph_and_node()
    input_node_dict = pruned_graph.get_input_node(mapped_node.name).dict()
    assert input_node_dict["name"] == "input_1"
    assert input_node_dict["parameters"]["type"] == "generic"


def test_sdk_code_generation(snowflake_database_table, update_fixtures):
    """Check SDK code generation for unsaved data"""
    check_sdk_code_generation(
        snowflake_database_table.frame,
        to_use_saved_data=False,
        fixture_path="tests/fixtures/sdk_code/generic_table.py",
        update_fixtures=update_fixtures,
        data_id=None,
    )

    # check that unsaved & saved version generate the same result for generic table
    sdk_code = snowflake_database_table.frame._generate_code(to_use_saved_data=True)
    assert sdk_code == snowflake_database_table.frame._generate_code(to_use_saved_data=False)
