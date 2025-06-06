"""
Integration tests related to entity tagging
"""

from datetime import datetime

import pandas as pd
import pytest

import featurebyte as fb
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY
from tests.util.deployment import deploy_and_get_online_features
from tests.util.helper import fb_assert_frame_equal


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
async def test_entity_different_dtypes(session_without_datasets, data_source, config):
    """
    Test registering an entity with different dtypes
    """

    def _unique_serving_name(serving_name: str) -> str:
        return "test_entity_different_dtypes__" + serving_name

    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-04-12 10:00:00",
            "2022-04-12 10:00:00",
            "2022-04-20 10:00:00",
            "2022-04-20 10:00:00",
        ]),
        "cust_id": [1000, 1001, 1000, 1001],
        "value": [1, 1, 2, 2],
    })
    df_dimension = pd.DataFrame({
        "cust_id": ["1000", "1001"],
        "dimension_value": ["A", "B"],
    })
    await session.register_table("test_entity_different_dtypes__scd", df_scd)
    await session.register_table("test_entity_different_dtypes__dimension", df_dimension)
    scd_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="test_entity_different_dtypes__scd",
    ).create_scd_table(
        name="test_entity_different_dtypes__scd_table",
        effective_timestamp_column="effective_ts",
        natural_key_column="cust_id",
    )
    dimension_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="test_entity_different_dtypes__dimension",
    ).create_dimension_table(
        name="test_entity_different_dtypes__dimension_table",
        dimension_id_column="cust_id",
    )

    # Tagging entities with different dtypes
    entity = fb.Entity.create(
        "test_entity_different_dtypes_entity", [_unique_serving_name("cust_id")]
    )
    scd_table["cust_id"].as_entity(entity.name)
    dimension_table["cust_id"].as_entity(entity.name)

    # Feature with cust_id entity
    view = scd_table.get_view()
    view = view.join(dimension_table.get_view())
    feature = view["dimension_value"].as_feature("my_feature")

    # Test preview
    preview_params = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2022-05-01", "2022-05-01"]),
        _unique_serving_name("cust_id"): ["1000", "1001"],
    })
    df_preview = feature.preview(preview_params)
    df_expected = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime(["2022-05-01", "2022-05-01"]),
        _unique_serving_name("cust_id"): ["1000", "1001"],
        "my_feature": ["A", "B"],
    })
    fb_assert_frame_equal(df_preview, df_expected)

    # Test online serving
    request_data = preview_params[[_unique_serving_name("cust_id")]].to_dict(orient="records")
    df_online_features = deploy_and_get_online_features(
        client=config.get_client(),
        feature_list=fb.FeatureList([feature], "test_entity_different_dtypes"),
        deploy_at=datetime(2022, 5, 1, 12, 0, 0),
        request_data=request_data,
    )
    df_expected = df_expected[[_unique_serving_name("cust_id"), "my_feature"]]
    fb_assert_frame_equal(df_online_features, df_expected)
