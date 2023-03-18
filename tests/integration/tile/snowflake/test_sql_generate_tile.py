"""
This module contains integration tests for tile generation stored procedure
"""
from datetime import datetime

import pytest
from snowflake.connector.errors import ProgrammingError

from featurebyte.enum import InternalName


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile(session):
    """
    Test normal generation of tiles
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f"WHERE {InternalName.TILE_START_DATE} >= '2022-06-05T23:48:00Z' "
        f"AND {InternalName.TILE_START_DATE} < '2022-06-05T23:58:00Z'"
    ).replace("'", "''")

    sql = (
        f"call SP_TILE_GENERATE("
        f"  '{tile_sql}',"
        f"  '{InternalName.TILE_START_DATE}',"
        f"  '{InternalName.TILE_LAST_START_DATE}',"
        f"  183,"
        f"  3,"
        f"  5,"
        f"  '{entity_col_names}',"
        f"  '{value_col_names}',"
        f"  '{value_col_types}',"
        f"  '{tile_id}',"
        f"  'OFFLINE',"
        f"  '2022-06-05T23:53:00Z')"
    )
    await session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    result = await session.execute_query(f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'")
    assert (
        result["LAST_TILE_START_DATE_OFFLINE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == "2022-06-05 23:53:00"
    )

    assert result["LAST_TILE_INDEX_OFFLINE"].iloc[0] == 5514910


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_no_data(session):
    """
    Test generation of tile with no tile table
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} "
        f"FROM {table_name} WHERE {InternalName.TILE_START_DATE} > '2022-06-05T23:58:00Z'"
    ).replace("'", "''")

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE', null)"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 0


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_non_exist_table(session):
    """
    Test generation of tile with error in tile query
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM NON_EXIST_TABLE"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{table_name}_TILE', 'ONLINE', null)"
    )

    with pytest.raises(ProgrammingError) as exc_info:
        await session.execute_query(sql)

    # make sure the error is about table not existing
    assert "Object 'NON_EXIST_TABLE' does not exist or not authorized." in str(exc_info.value)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_new_value_column(session):
    """
    Test normal generation of tiles
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} "
        f"WHERE {InternalName.TILE_START_DATE} >= '2022-06-05T23:48:00Z' "
        f"AND {InternalName.TILE_START_DATE} < '2022-06-05T23:58:00Z'"
    ).replace("'", "''")

    sql = (
        f"call SP_TILE_GENERATE("
        f"  '{tile_sql}',"
        f"  '{InternalName.TILE_START_DATE}',"
        f"  '{InternalName.TILE_LAST_START_DATE}',"
        f"  183,"
        f"  3,"
        f"  5,"
        f"  '{entity_col_names}',"
        f"  '{value_col_names}',"
        f"  '{value_col_types}',"
        f"  '{tile_id}',"
        f"  'OFFLINE',"
        f"  '2022-06-05T23:53:00Z')"
    )
    await session.execute_query(sql)

    sql = f"SELECT {value_col_names} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["VALUE_COLUMN_NAMES"].iloc[0] == "VALUE"

    value_col_names_2 = "VALUE,VALUE_2"
    value_col_types_2 = "FLOAT,FLOAT"
    tile_sql_2 = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names_2} FROM {table_name} "
        f"WHERE {InternalName.TILE_START_DATE} >= '2022-06-05T23:48:00Z' "
        f"AND {InternalName.TILE_START_DATE} < '2022-06-05T23:58:00Z'"
    ).replace("'", "''")
    with pytest.raises(ProgrammingError) as excinfo:
        sql = f"SELECT {value_col_names_2} FROM {tile_id}"
        await session.execute_query(sql)
    assert "invalid identifier 'VALUE_2'" in str(excinfo.value)

    sql = (
        f"call SP_TILE_GENERATE("
        f"  '{tile_sql_2}',"
        f"  '{InternalName.TILE_START_DATE}',"
        f"  '{InternalName.TILE_LAST_START_DATE}',"
        f"  183,"
        f"  3,"
        f"  5,"
        f"  '{entity_col_names}',"
        f"  '{value_col_names_2}',"
        f"  '{value_col_types_2}',"
        f"  '{tile_id}',"
        f"  'OFFLINE',"
        f"  '2022-06-05T23:53:00Z')"
    )
    await session.execute_query(sql)

    sql = f"SELECT {value_col_names_2} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["VALUE_COLUMN_NAMES"].iloc[0] == "VALUE,VALUE_2"
