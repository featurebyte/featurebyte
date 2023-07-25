"""
This module contains integration tests for online feature store
"""
from datetime import datetime

import numpy as np
import pytest

from featurebyte.enum import InternalName
from featurebyte.models.online_store_compute_query import OnlineStoreComputeQueryModel
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore


@pytest.fixture
def online_store_cleanup_service(app_container):
    """
    Fixture to cleanup online store table
    """
    return app_container.online_store_cleanup_service


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
    session,
    persistent,
    tile_task_prep_spark,
    base_sql_model,
    online_store_table_version_service,
    online_store_compute_query_service,
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
        online_store_compute_query_service=online_store_compute_query_service,
    )
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]

    number_records = 2
    quote_feature_name = base_sql_model.quote_column(feature_name)
    new_sql_query = f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, 100.0 as {quote_feature_name} from TEMP_TABLE limit {number_records}
        )
        """
    await persistent._update_one(
        "online_store_compute_query",
        {"tile_id": tile_id},
        {"$set": {"sql": new_sql_query}},
    )

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
        online_store_compute_query_service=online_store_compute_query_service,
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
    session,
    tile_task_prep_spark,
    base_sql_model,
    online_store_table_version_service,
    online_store_compute_query_service,
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
        online_store_compute_query_service=online_store_compute_query_service,
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
    new_sql_query = f"""
        select
          {entity_col_names},
          '{new_feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, cast(value_2 as float) as {quote_feature_name} from TEMP_TABLE limit 2
        )
        """
    await online_store_compute_query_service.create_document(
        OnlineStoreComputeQueryModel(
            tile_id=tile_id,
            aggregation_id=agg_id,
            result_name=new_feature_name,
            result_type="FLOAT",
            sql=new_sql_query,
            table_name=feature_store_table_name,
            serving_names=entity_col_names.split(","),
        )
    )

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
        online_store_compute_query_service=online_store_compute_query_service,
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
    session,
    persistent,
    tile_task_prep_spark,
    base_sql_model,
    online_store_table_version_service,
    online_store_compute_query_service,
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
        online_store_compute_query_service=online_store_compute_query_service,
    )
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 2
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0]
    assert result["PRODUCT_ACTION"].tolist() == ["view", "view"]

    quote_feature_name = base_sql_model.quote_column(feature_name)
    new_select_sql = f"""
        select
          {entity_col_names},
          '{feature_name}' as {InternalName.ONLINE_STORE_RESULT_NAME_COLUMN},
          {quote_feature_name} as {InternalName.ONLINE_STORE_VALUE_COLUMN}
        FROM (
          select {entity_col_names}, 100.0 as {quote_feature_name} from TEMP_TABLE ORDER BY __FB_TILE_START_DATE_COLUMN ASC limit 2
        )
        """
    await persistent._update_one(
        "online_store_compute_query",
        {"tile_id": tile_id},
        {"$set": {"sql": new_select_sql}},
    )

    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
        online_store_compute_query_service=online_store_compute_query_service,
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


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_online_store_table_cleanup(
    session,
    persistent,
    tile_task_prep_spark,
    online_store_table_version_service,
    online_store_compute_query_service,
    online_store_cleanup_service,
    feature_store,
):
    """
    Test cleaning up of online store tables
    """

    tile_id, agg_id, feature_store_table_name, feature_name, entity_col_names = tile_task_prep_spark
    date_ts_str = datetime.now().isoformat()[:-3] + "Z"

    # Populate the online store table multiple times to get different versions
    tile_online_store_ins = TileScheduleOnlineStore(
        session=session,
        aggregation_id=agg_id,
        job_schedule_ts_str=date_ts_str,
        online_store_table_version_service=online_store_table_version_service,
        online_store_compute_query_service=online_store_compute_query_service,
    )
    await tile_online_store_ins.execute()
    await tile_online_store_ins.execute()
    await tile_online_store_ins.execute()

    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 6
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6, 3, 6, 3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [0, 0, 1, 1, 2, 2]
    assert result["PRODUCT_ACTION"].tolist() == ["view"] * 6

    # Run clean up
    await online_store_cleanup_service.run_cleanup(
        feature_store_id=feature_store.id, online_store_table_name=feature_store_table_name
    )

    # Check that the latest two versions are kept
    result = await retrieve_online_store_content(session, feature_store_table_name, feature_name)
    assert len(result) == 4
    assert result[InternalName.ONLINE_STORE_VALUE_COLUMN].tolist() == [3, 6, 3, 6]
    assert result[InternalName.ONLINE_STORE_VERSION_COLUMN].tolist() == [1, 1, 2, 2]
    assert result["PRODUCT_ACTION"].tolist() == ["view"] * 4
