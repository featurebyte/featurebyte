"""
This module contains integration tests for online feature store
"""
from datetime import datetime

import numpy as np
import pytest

from featurebyte.enum import InternalName
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore


async def retrieve_online_store_content(session, feature_store_table_name, aggregation_result_name):
    """
    Helper function to retrieve online store content
    """
    sql = f"""
        SELECT * FROM {feature_store_table_name}
        WHERE {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN} = '{aggregation_result_name}'
        ORDER BY {InternalName.ONLINE_STORE_VERSION_COLUMN}, __FB_TILE_START_DATE_COLUMN
        """
    return await session.execute_query(sql)


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__update_feature_value(
    session, tile_task_prep_spark, base_sql_model, online_store_table_version_service
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]

    number_records = 2
    quote_feature_name = base_sql_model.quote_column(feature_name)
    adapter = get_sql_adapter(session.source_type)
    new_sql_query = adapter.escape_quote_char(
        f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, 100.0 as {quote_feature_name} from TEMP_TABLE limit {number_records}
        )
        """
    )
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING SET SQL_QUERY = '{new_sql_query}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 4
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6, 100, 100]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0, 1, 1]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view", "view", "view"]


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_update_feature_store__insert_with_new_feature_column(
    session, tile_task_prep_spark, base_sql_model, online_store_table_version_service
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    # verify existing feature store table
    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]

    new_feature_name = feature_name + "_2"
    quote_feature_name = base_sql_model.quote_column(new_feature_name)
    adapter = get_sql_adapter(session.source_type)
    new_sql_query = adapter.escape_quote_char(
        f"""
        select
          {entity_col_names},
          '{new_feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, cast(value_2 as float) as {quote_feature_name} from TEMP_TABLE limit 2
        )
        """
    )
    insert_new_mapping_sql = f"""
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
                '{agg_id}',
                '{new_feature_name}',
                'FLOAT',
                '{new_sql_query}',
                '{feature_store_table_name}',
                '{entity_col_names}',
                false,
                current_timestamp()
            )
    """
    await session.execute_query(insert_new_mapping_sql)

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    # Check existing aggregation which is updated twice now
    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 4
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6, 3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0, 1, 1]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view", "view", "view"]

    # Check new aggregation which is updated once
    result = await retrieve_online_store_content(
        session, feature_store_table_name, new_feature_name
    )
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_online_feature_store__change_entity_universe(
    session, tile_task_prep_spark, base_sql_model, online_store_table_version_service
):
    """
    Test the stored procedure for updating feature store
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]

    quote_feature_name = base_sql_model.quote_column(feature_name)
    adapter = get_sql_adapter(session.source_type)
    new_select_sql = adapter.escape_quote_char(
        f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, 100.0 as {quote_feature_name} from TEMP_TABLE ORDER BY __FB_TILE_START_DATE_COLUMN ASC limit 2
        )
        """
    )
    update_mapping_sql = f"""
        UPDATE ONLINE_STORE_MAPPING
        SET SQL_QUERY = '{new_select_sql}'
        WHERE TILE_ID = '{tile_id}'
"""
    await session.execute_query(update_mapping_sql)

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
    )
    await tile_online_store_ins.execute()

    # Version 1 doesn't have the old entity "view" since it's not in the universe
    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    result = result[result["VERSION"] == 1]
    assert len(result) == 2
    np.testing.assert_allclose(
        result[InternalName.ONLINE_STORE_VALUE_COLUMN],
        [100, 100],
    )
    assert result["PRODUCT_ACTION"].tolist() == ["action", "action"]
