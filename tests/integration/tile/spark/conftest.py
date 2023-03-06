"""
Tile Tests for Spark Session
"""
from datetime import datetime

import pytest_asyncio

from featurebyte.tile.spark_tile import TileManagerSpark


@pytest_asyncio.fixture(name="tile_task_prep_spark")
async def tile_task_online_store_prep(session):
    assert session.source_type == "spark"

    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    aggregation_id = f"some_agg_id_{suffix}"

    number_records = 2
    insert_mapping_sql = f"""
            insert into ONLINE_STORE_MAPPING(
                TILE_ID,
                AGGREGATION_ID,
                RESULT_ID,
                RESULT_TYPE,
                SQL_QUERY,
                ONLINE_STORE_TABLE_NAME,
                ENTITY_COLUMN_NAMES,
                IS_DELETED,
                CREATED_AT
            )
            values (
                '{tile_id}',
                '{aggregation_id}',
                '{feature_name}',
                'FLOAT',
                'select {entity_col_names}, cast(value_2 as float) as {feature_name} from {table_name} limit {number_records}',
                '{feature_store_table_name}',
                '{entity_col_names}',
                false,
                current_timestamp()
            )
    """
    await session.execute_query(insert_mapping_sql)

    sql = f"SELECT * FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == tile_id
    assert result["RESULT_ID"].iloc[0] == feature_name

    yield tile_id, aggregation_id, feature_store_table_name, feature_name, entity_col_names

    await session.execute_query("DELETE FROM ONLINE_STORE_MAPPING")
    await session.execute_query(f"DROP TABLE IF EXISTS {feature_store_table_name}")


@pytest_asyncio.fixture(name="tile_manager")
async def tile_manager_fixture(session, tile_spec):
    assert session.source_type == "spark"

    yield TileManagerSpark(session=session)

    await session.execute_query(f"DROP TABLE IF EXISTS {tile_spec.aggregation_id}_ENTITY_TRACKER")
