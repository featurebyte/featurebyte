"""
This module contains integration tests for tile generation stored procedure
"""
import pytest
from snowflake.connector.errors import ProgrammingError


def test_generate_tile(fb_db_session):
    """
    Test normal generation of tiles
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM {table_name}"

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {table_name}_TILE"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100


def test_generate_tile_no_data(fb_db_session):
    """
    Test generation of tile with no tile data
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = (
        f"SELECT {col_names} FROM {table_name} WHERE TILE_START_TS > \\'2022-06-05T23:58:00Z\\'"
    )

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {table_name}_TILE"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 0


def test_generate_tile_non_exist_table(fb_db_session):
    """
    Test generation of tile with error in tile query
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM NON_EXIST_TABLE"

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE')"

    with pytest.raises(ProgrammingError):
        fb_db_session.execute_query(sql)
