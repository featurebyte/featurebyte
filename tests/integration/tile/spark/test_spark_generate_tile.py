"""
Tile Generate tests for Spark Session
"""
from datetime import datetime

import pytest

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate import TileGenerate


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile(session):
    """
    Test normal generation of tiles
    """

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2

    result = await session.execute_query(f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'")
    assert result["VALUE_COLUMN_NAMES"].iloc[0] == "VALUE"
    assert result["VALUE_COLUMN_TYPES"].iloc[0] == "FLOAT"


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_no_data(session):
    """
    Test generation of tile with no tile data
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} "
        f"FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') > '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 0


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_new_value_column(session):
    """
    Test normal generation of tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    entity_col_names_str = ",".join([f"`{col}`" for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT {value_col_names_str} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["VALUE_COLUMN_NAMES"].iloc[0] == "VALUE"

    value_col_names_2 = ["VALUE", "VALUE_2"]
    value_col_types_2 = ["FLOAT", "FLOAT"]
    value_col_names_2_str = ",".join(value_col_names_2)
    tile_sql_2 = (
        f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_2_str} FROM {table_name} "
        f"WHERE date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') >= '2022-06-05 23:48:00' "
        f"AND date_format({InternalName.TILE_START_DATE}, 'yyyy-MM-dd HH:mm:ss') < '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        spark_session=session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql_2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names_2,
        value_column_types=value_col_types_2,
        tile_type="OFFLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT {value_col_names_2_str} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    sql = f"SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["VALUE_COLUMN_NAMES"].iloc[0] == "VALUE,VALUE_2"
