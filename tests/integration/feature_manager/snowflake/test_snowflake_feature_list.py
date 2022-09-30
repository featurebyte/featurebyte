"""
This module contains integration tests for FeatureListManagerSnowflake
"""
import json

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.enum import InternalName
from featurebyte.exception import DuplicatedRegistryError
from featurebyte.models.base import VersionIdentifier
from featurebyte.models.feature_list import FeatureListStatus


@pytest.mark.asyncio
async def test_insert_feature_list_registry(
    snowflake_session, snowflake_feature_list, feature_list_manager
):
    """
    Test insert_feature_list_registry
    """
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    result = await snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"

    expected_df = pd.DataFrame.from_dict(
        {
            "NAME": ["feature_list1"],
            "VERSION": ["v1"],
            "STATUS": ["PUBLIC_DRAFT"],
        }
    )
    result_df = result[
        [
            "NAME",
            "VERSION",
            "STATUS",
        ]
    ]
    assert_frame_equal(expected_df, result_df)

    expected_fv = {"feature": "sum_30m", "version": "v1"}
    result_fv = json.loads(result["FEATURE_VERSIONS"].iloc[0])[0]
    assert expected_fv == result_fv


@pytest.mark.asyncio
async def test_insert_feature_list_registry_duplicate(
    snowflake_session, snowflake_feature_list, feature_list_manager
):
    """
    Test insert_feature_list_registry duplicate with exception
    """
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    result = await snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"

    with pytest.raises(DuplicatedRegistryError) as excinfo:
        await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    assert (
        str(excinfo.value)
        == f"FeatureList version already exist for {snowflake_feature_list.name} with version "
        f"{snowflake_feature_list.version.to_str()}"
    )


@pytest.mark.asyncio
async def test_retrieve_feature_list_registry(snowflake_feature_list, feature_list_manager):
    """
    Test retrieve_features
    """
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)
    f_reg_df = await feature_list_manager.retrieve_feature_list_registries(snowflake_feature_list)
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"

    f_reg_df = await feature_list_manager.retrieve_feature_list_registries(
        feature_list=snowflake_feature_list,
        version=VersionIdentifier(name="v1"),
    )
    assert len(f_reg_df) == 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"


@pytest.mark.asyncio
async def test_retrieve_feature_list_registry_multiple(
    snowflake_feature_list, feature_list_manager
):
    """
    Test retrieve_features return multiple features
    """
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    snowflake_feature_list.__dict__["version"] = VersionIdentifier(name="v2")
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)

    f_reg_df = await feature_list_manager.retrieve_feature_list_registries(snowflake_feature_list)
    assert len(f_reg_df) > 1
    assert f_reg_df.iloc[0]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[0]["VERSION"] == "v1"
    assert f_reg_df.iloc[1]["NAME"] == "feature_list1"
    assert f_reg_df.iloc[1]["VERSION"] == "v2"


@pytest.mark.asyncio
async def test_update_feature_list_registry(
    snowflake_session, snowflake_feature_list, feature_list_manager
):
    """
    Test update_feature_registry
    """
    await feature_list_manager.insert_feature_list_registry(snowflake_feature_list)
    result = await snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["STATUS"] == "PUBLIC_DRAFT"

    snowflake_feature_list.__dict__["status"] = FeatureListStatus.PUBLISHED.value
    await feature_list_manager.update_feature_list_registry(snowflake_feature_list)

    result = await snowflake_session.execute_query("SELECT * FROM FEATURE_LIST_REGISTRY")
    assert len(result) == 1
    assert result.iloc[0]["NAME"] == "feature_list1"
    assert result.iloc[0]["VERSION"] == "v1"
    assert result.iloc[0]["STATUS"] == "PUBLISHED"


@pytest.mark.asyncio
async def test_generate_tiles_on_demand(snowflake_session, snowflake_tile, feature_list_manager):
    """
    Test generate_tiles_on_demand
    """
    temp_entity_table = "TEMP_ENTITY_TRACKER_1"
    last_tile_start_date_1 = "2022-07-06 10:52:14"
    await snowflake_session.execute_query(
        f"CREATE TEMPORARY TABLE {temp_entity_table} (PRODUCT_ACTION VARCHAR, CUST_ID VARCHAR, LAST_TILE_START_DATE TIMESTAMP_TZ)"
    )
    await snowflake_session.execute_query(
        f"INSERT INTO {temp_entity_table} VALUES ('P1', 'C1', '{last_tile_start_date_1}') "
    )

    snowflake_tile.tile_sql = snowflake_tile.tile_sql.replace(
        InternalName.TILE_START_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:33:00'"
    ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, "'2022-06-05 23:58:00'")

    await feature_list_manager.generate_tiles_on_demand([(snowflake_tile, temp_entity_table)])

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = await snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5

    sql = f"SELECT * FROM {snowflake_tile.aggregation_id}_ENTITY_TRACKER ORDER BY PRODUCT_ACTION"
    result = await snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["PRODUCT_ACTION"].iloc[0] == "P1"
    assert result["CUST_ID"].iloc[0] == "C1"
    assert (
        result["LAST_TILE_START_DATE"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        == last_tile_start_date_1
    )
