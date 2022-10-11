"""
Unit test for DatabaseTable
"""
import pandas as pd

from featurebyte.enum import DBVarType, TableDataType


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
            "event_timestamp": DBVarType.TIMESTAMP,
            "created_at": DBVarType.TIMESTAMP,
            "cust_id": DBVarType.INT,
        }
    )
    pd.testing.assert_series_equal(snowflake_database_table.dtypes, expected_dtypes)


def test_database_table_node_parameters(snowflake_database_table):
    """Test database table node parameters"""
    node_params = snowflake_database_table.node.parameters
    assert node_params.type == TableDataType.GENERIC


def test_database_table_get_input_node(snowflake_database_table):
    """Test database table get input node"""
    pruned_graph, mapped_node = snowflake_database_table.extract_pruned_graph_and_node()
    input_node_dict = pruned_graph.get_input_node(mapped_node.name).dict()
    assert input_node_dict["name"] == "input_1"
    assert input_node_dict["parameters"]["type"] == "generic"
