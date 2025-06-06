"""
Tile Generate tests for Spark Session
"""

import asyncio
import threading
from datetime import datetime
from queue import Queue

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import InternalName
from featurebyte.sql.tile_generate import TileGenerate
from tests.integration.tile.hepler import format_timestamp_expr


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile(
    session,
    base_sql_model,
    tile_registry_service,
    warehouse_table_service,
    deployed_tile_table_service,
):
    """
    Test normal generation of tiles
    """

    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"

    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    fmt_timestamp_expr = format_timestamp_expr(session, InternalName.TILE_START_DATE)
    tile_sql = (
        f"SELECT index,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f"WHERE {fmt_timestamp_expr} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f"AND {fmt_timestamp_expr} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_ts_str="2022-06-05 23:48:00",
        tile_end_ts_str="2022-06-05 23:58:00",
        update_last_run_metadata=False,
        aggregation_id=agg_id,
        feature_store_id=ObjectId(),
        tile_registry_service=tile_registry_service,
        warehouse_table_service=warehouse_table_service,
        deployed_tile_table_service=deployed_tile_table_service,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_no_data(
    session,
    base_sql_model,
    tile_registry_service,
    warehouse_table_service,
    deployed_tile_table_service,
):
    """
    Test generation of tile with no tile table
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    fmt_timestamp_expr = format_timestamp_expr(session, InternalName.TILE_START_DATE)
    tile_sql = (
        f"SELECT index,{entity_col_names_str},{value_col_names_str} "
        f"FROM {table_name} "
        f"WHERE {fmt_timestamp_expr} > '2022-06-05 23:58:00'"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_ts_str=None,
        tile_end_ts_str=None,
        update_last_run_metadata=False,
        aggregation_id=agg_id,
        feature_store_id=ObjectId(),
        tile_registry_service=tile_registry_service,
        warehouse_table_service=warehouse_table_service,
        deployed_tile_table_service=deployed_tile_table_service,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 0


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tile_new_value_column(
    session,
    base_sql_model,
    tile_registry_service,
    warehouse_table_service,
    deployed_tile_table_service,
):
    """
    Test normal generation of tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    fmt_timestamp_expr = format_timestamp_expr(session, InternalName.TILE_START_DATE)
    tile_sql = (
        f"SELECT index,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f"WHERE {fmt_timestamp_expr} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f"AND {fmt_timestamp_expr} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="OFFLINE",
        tile_start_ts_str="2022-06-05 23:48:00",
        tile_end_ts_str="2022-06-05 23:58:00",
        update_last_run_metadata=False,
        aggregation_id=agg_id,
        feature_store_id=ObjectId(),
        tile_registry_service=tile_registry_service,
        warehouse_table_service=warehouse_table_service,
        deployed_tile_table_service=deployed_tile_table_service,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT {value_col_names_str} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2

    value_col_names_2 = ["VALUE", "VALUE_2"]
    value_col_types_2 = ["FLOAT", "FLOAT"]
    value_col_names_2_str = ",".join(value_col_names_2)
    tile_sql_2 = (
        f"SELECT index,{entity_col_names_str},{value_col_names_2_str} FROM {table_name} "
        f"WHERE {fmt_timestamp_expr} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f"AND {fmt_timestamp_expr} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql_2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names_2,
        value_column_types=value_col_types_2,
        tile_type="OFFLINE",
        tile_start_ts_str="2022-06-05 23:48:00",
        tile_end_ts_str="2022-06-05 23:58:00",
        update_last_run_metadata=False,
        aggregation_id=agg_id,
        feature_store_id=ObjectId(),
        tile_registry_service=tile_registry_service,
        warehouse_table_service=warehouse_table_service,
        deployed_tile_table_service=deployed_tile_table_service,
    )

    await tile_generate_ins.execute()

    sql = f"SELECT {value_col_names_2_str} FROM {tile_id}"
    result = await session.execute_query(sql)
    assert len(result) == 2


@pytest.mark.asyncio
async def test_generate_tile_concurrent(
    session,
    base_sql_model,
    tile_registry_service,
    warehouse_table_service,
    deployed_tile_table_service,
):
    """
    Test multiple tasks generating the same tile table concurrently
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"
    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    fmt_timestamp_expr = format_timestamp_expr(session, InternalName.TILE_START_DATE)

    num_parallel_tile_tasks = 5
    tile_sql_template = (
        f"SELECT index,{entity_col_names_str},{value_col_names_str} FROM {table_name} "
        f"WHERE {fmt_timestamp_expr} >= '{{start_date}}' "
        f"AND {fmt_timestamp_expr} < '{{end_date}}'"
    )
    start_date = pd.Timestamp("2022-06-05 15:00:00")
    tile_sqls = [
        tile_sql_template.format(
            start_date=start_date + pd.Timedelta(index, unit="h"),
            end_date=start_date + pd.Timedelta(index + 1, unit="h"),
        )
        for index in range(num_parallel_tile_tasks)
    ]

    async def run_inner(session_obj, index):
        """
        Simulate a tile task on a tile table
        """
        print(f"Running parallel tile task {index}")
        session_obj = await session_obj.clone_if_not_threadsafe()
        tile_generate_ins = TileGenerate(
            session=session_obj,
            tile_id=tile_id,
            time_modulo_frequency_second=183,
            blind_spot_second=3,
            frequency_minute=5,
            sql=tile_sqls[index],
            entity_column_names=entity_col_names,
            value_column_names=value_col_names,
            value_column_types=value_col_types,
            tile_type="OFFLINE",
            tile_start_ts_str=None,
            tile_end_ts_str=None,
            update_last_run_metadata=False,
            aggregation_id=agg_id,
            feature_store_id=ObjectId(),
            tile_registry_service=tile_registry_service,
            warehouse_table_service=warehouse_table_service,
            deployed_tile_table_service=deployed_tile_table_service,
        )
        await tile_generate_ins.execute()

    async def run(session_obj, value, out):
        try:
            await run_inner(session_obj, value)
        except Exception as e:  # pylint: disable=broad-exception-caught
            out.put(e)

    out = Queue()
    threads = []
    for i in range(2):
        t = threading.Thread(target=asyncio.run, args=(run(session, i, out),))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    if not out.empty():
        raise out.get()

    sql = f"SELECT COUNT(*) FROM {tile_id}"
    result = await session.execute_query(sql)
    assert result.iloc[0][0] == 16
