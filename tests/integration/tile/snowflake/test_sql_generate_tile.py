"""
This module contains integration tests for tile generation stored procedure
"""
from datetime import datetime

import pytest
from snowflake.connector.errors import ProgrammingError

from featurebyte.enum import InternalName


def test_generate_tile(snowflake_session):
    """
    Test normal generation of tiles
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {col_names} FROM {table_name} "
        f"WHERE {InternalName.TILE_START_DATE} >= \\'2022-06-05T23:48:00Z\\' "
        f"AND {InternalName.TILE_START_DATE} < \\'2022-06-05T23:58:00Z\\'"
    )

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{tile_id}', 'OFFLINE', '2022-06-05T23:53:00Z', '{InternalName.TILE_START_DATE}')"
    snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    result = snowflake_session.execute_query(
        f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    )
    assert (
        result["LAST_TILE_START_DATE_OFFLINE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == "2022-06-05 23:53:00"
    )
    assert result["LAST_TILE_INDEX_OFFLINE"].iloc[0] == 5514910


def test_generate_tile_no_data(snowflake_session):
    """
    Test generation of tile with no tile data
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {col_names} FROM {table_name} WHERE {InternalName.TILE_START_DATE} > \\'2022-06-05T23:58:00Z\\'"

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{tile_id}', 'ONLINE', null, '{InternalName.TILE_START_DATE}')"
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 0


def test_generate_tile_non_exist_table(snowflake_session):
    """
    Test generation of tile with error in tile query
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM NON_EXIST_TABLE"

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE', 'ONLINE', null, '{InternalName.TILE_START_DATE}')"

    with pytest.raises(ProgrammingError) as exc_info:
        snowflake_session.execute_query(sql)

    # make sure the error is about table not existing
    assert "Object 'NON_EXIST_TABLE' does not exist or not authorized." in str(exc_info.value)
