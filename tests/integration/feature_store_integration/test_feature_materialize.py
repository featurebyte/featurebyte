"""
Tests for feature materialization service
"""
import json
import os
import time
from unittest.mock import patch

import freezegun
import pandas as pd
import pytest

import featurebyte as fb
from featurebyte.logging import get_logger
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from tests.util.helper import assert_dict_approx_equal

logger = get_logger(__name__)


@pytest.fixture(name="always_enable_feast_integration", scope="module", autouse=True)
def always_enable_feast_integration_fixture():
    """
    Enable feast integration for all tests in this module
    """
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True"}):
        yield


@pytest.fixture(name="features", scope="module")
def features_fixture(event_table, scd_table):  # pylint: disable=too-many-locals
    """
    Fixture for feature
    """
    event_view = event_table.get_view()

    # Feature saved but not deployed
    feature_0 = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["666h"],
        feature_names=["EXTERNAL_FS_FEATURE_NOT_DEPLOYED"],
    )
    feature_0.save()

    # Window aggregate feature
    feature_1 = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24h"],
        feature_names=["EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h"],
    )["EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h"]

    # Window aggregate feature with more post-processing
    feature_2 = feature_1 * 100
    feature_2.name = feature_1.name + "_TIMES_100"

    # Feature with a different entity
    filtered_event_view = event_view[event_view["PRODUCT_ACTION"].notnull()]
    feature_3 = filtered_event_view.groupby("PRODUCT_ACTION").aggregate_over(
        None,
        method="count",
        windows=["7d"],
        feature_names=["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"],
    )["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"]

    # Complex feature with multiple unrelated entities and request column
    feature_4 = feature_1 + feature_3 + fb.RequestColumn.point_in_time().dt.day.sin()
    feature_4.name = "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE"

    # Feature without entity
    feature_5 = filtered_event_view.groupby([]).aggregate_over(
        None,
        method="count",
        windows=["7d"],
        feature_names=["EXTERNAL_FS_COUNT_OVERALL_7d"],
    )["EXTERNAL_FS_COUNT_OVERALL_7d"]

    # Dict feature
    feature_6 = event_view.groupby("ÜSER ID", category="PRODUCT_ACTION").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["7d"],
        feature_names=["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"],
    )["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"]

    cross_aggregate_feature2 = event_view.groupby(
        ["CUST_ID"], category="PRODUCT_ACTION"
    ).aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24d"],
        feature_names=["amount_sum_across_action_24d"],
    )[
        "amount_sum_across_action_24d"
    ]

    feature_7 = feature_6.cd.cosine_similarity(cross_aggregate_feature2)
    feature_7.name = "EXTERNAL_FS_COSINE_SIMILARITY"

    feature_8 = event_view.groupby("ÜSER ID").aggregate_over(
        "EMBEDDING_ARRAY",
        method="avg",
        windows=["24h"],
        feature_names=["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"],
    )["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"]

    vec_agg_feature2 = event_view.groupby("CUST_ID").aggregate_over(
        "EMBEDDING_ARRAY",
        method="avg",
        windows=["48h"],
        feature_names=["EXTERNAL_FS_ARRAY_AVG_BY_CUST_ID_48h"],
    )["EXTERNAL_FS_ARRAY_AVG_BY_CUST_ID_48h"]

    feature_9 = feature_8.vec.cosine_similarity(vec_agg_feature2)
    feature_9.name = "EXTERNAL_FS_COSINE_SIMILARITY_VEC"

    scd_view = scd_table.get_view()
    feature_10 = scd_view["User Status"].as_feature("User Status Feature")
    feature_11 = (
        scd_view.groupby("User Status")
        .aggregate_asat(method="count", feature_name="Current Number of Users With This Status")
        .astype(float)
    )
    feature_11.name = "Current Number of Users With This Status"

    # Save all features to be deployed
    features = [
        feature_1,
        feature_2,
        feature_3,
        feature_4,
        feature_5,
        feature_6,
        feature_7,
        feature_8,
        feature_9,
        feature_10,
        feature_11,
    ]
    for feature in features:
        feature.save()
        feature.update_readiness("PRODUCTION_READY")

    return features


@pytest.fixture(name="deployed_feature_list", scope="module")
def deployed_features_list_fixture(features):
    """
    Fixture for deployed feature list
    """
    feature_list = fb.FeatureList(features, name="EXTERNAL_FS_FEATURE_LIST")
    feature_list.save()
    with patch(
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=pd.Timestamp("2001-01-02 12:00:00").to_pydatetime(),
    ):
        deployment = feature_list.deploy()
        deployment.enable()
    yield deployment
    deployment.disable()


@pytest.fixture(name="default_feature_job_setting")
def default_feature_job_setting_fixture(event_table):
    """
    Fixture for default feature job setting
    """
    return event_table.default_feature_job_setting


async def check_feature_tables_populated(session, feature_tables):
    """
    Check feature tables are populated correctly
    """
    for feature_table in feature_tables:
        df = await session.execute_query(f'SELECT * FROM "{feature_table.name}"')

        # Should not be empty
        assert df.shape[0] > 0

        # Should have all the serving names and output columns tracked in OfflineStoreFeatureTable
        assert set(df.columns.tolist()) == set(
            ["__feature_timestamp"]
            + feature_table.serving_names
            + feature_table.output_column_names
        )


async def check_feast_registry(app_container):
    """
    Check feast registry is populated correctly
    """
    feast_registry = await app_container.feast_registry_service.get_feast_registry_for_catalog()
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )

    # Check feature views and feature services
    assert {fv.name for fv in feature_store.list_feature_views()} == {
        "fb_entity_overall_fjs_3600_1800_1800_ttl",
        "fb_entity_product_action_fjs_3600_1800_1800_ttl",
        "fb_entity_cust_id_fjs_3600_1800_1800_ttl",
        "fb_entity_üserid_fjs_3600_1800_1800_ttl",
        "fb_entity_üserid_fjs_86400_0_0",
        "fb_entity_user_status_fjs_86400_0_0",
    }
    assert {fs.name for fs in feature_store.list_feature_services()} == {"EXTERNAL_FS_FEATURE_LIST"}

    # Check feast materialize and get_online_features
    feature_service = feature_store.get_feature_service("EXTERNAL_FS_FEATURE_LIST")
    online_features = feature_store.get_online_features(
        features=feature_service,
        entity_rows=[
            {
                "üser id": 1,
                "cust_id": 761,
                "user_status": "STÀTUS_CODE_39",
                "PRODUCT_ACTION": "detail",
                "POINT_IN_TIME": pd.Timestamp("2001-01-02 12:00:00"),
            }
        ],
    ).to_dict()
    online_features["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"] = [
        json.loads(online_features["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"][0])
    ]
    online_features["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = [
        json.loads(online_features["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"][0])
    ]
    expected = {
        "üser id": ["1"],
        "cust_id": ["761"],
        "PRODUCT_ACTION": ["detail"],
        "user_status": ["STÀTUS_CODE_39"],
        "User Status Feature": ["STÀTUS_CODE_39"],
        "Current Number of Users With This Status": [1.0],
        "EXTERNAL_FS_COUNT_OVERALL_7d": [149.0],
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": [43.0],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": [475.38],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": [47538.0],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": [519.2892974268257],
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": [
            {
                "__MISSING__": 234.77,
                "detail": 235.24,
                "purchase": 225.78,
                "rëmove": 11.39,
                "àdd": 338.51,
            }
        ],
        "EXTERNAL_FS_COSINE_SIMILARITY": [0],
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            [
                0.41825654595626777,
                0.3459365542614712,
                0.5725510296687925,
                0.424307035963231,
                0.4930920411475924,
                0.4502761817462119,
                0.3192654242159095,
                0.40611594238301874,
                0.649378423267523,
                0.3857218591399362,
            ]
        ],
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": [0.8593524820234559],
    }
    assert_dict_approx_equal(online_features, expected)


@freezegun.freeze_time("2001-01-02 12:00:00")
def check_online_features(deployment, config):
    """
    Check online features are populated correctly
    """
    client = config.get_client()

    entity_serving_names = [
        {
            "üser id": 1,
            "cust_id": 761,
            "PRODUCT_ACTION": "detail",
            # Note: shouldn't have to provide this once parent entity lookup is supported (via child
            # entity üser id)
            "user_status": "STÀTUS_CODE_39",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    tic = time.time()
    res = client.post(
        f"/deployment/{deployment.id}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 200
    elapsed = time.time() - tic
    logger.info("online_features elapsed: %fs", elapsed)
    feat_dict = res.json()["features"][0]
    feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"] = json.loads(
        feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"]
    )
    feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = json.loads(
        feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"]
    )
    expected = {
        "üser id": "1",
        "cust_id": "761",
        "PRODUCT_ACTION": "detail",
        "user_status": "STÀTUS_CODE_39",
        "User Status Feature": "STÀTUS_CODE_39",
        "Current Number of Users With This Status": 1.0,
        "EXTERNAL_FS_COUNT_OVERALL_7d": 149.0,
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": 43.0,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": 475.38,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": 47538.0,
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": 519.2892974268257,
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": {
            "__MISSING__": 234.77,
            "detail": 235.24,
            "purchase": 225.78,
            "rëmove": 11.39,
            "àdd": 338.51,
        },
        "EXTERNAL_FS_COSINE_SIMILARITY": 0,
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            0.41825654595626777,
            0.3459365542614712,
            0.5725510296687925,
            0.424307035963231,
            0.4930920411475924,
            0.4502761817462119,
            0.3192654242159095,
            0.40611594238301874,
            0.649378423267523,
            0.3857218591399362,
        ],
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": 0.8593524820234559,
    }
    assert_dict_approx_equal(feat_dict, expected)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_feature_materialize_service(
    app_container,
    session,
    user_entity,
    customer_entity,
    product_action_entity,
    status_entity,
    deployed_feature_list,
    config,
):
    """
    Test FeatureMaterializeService
    """
    _ = deployed_feature_list

    primary_entity_to_feature_table = {}
    async for feature_table in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={},
    ):
        primary_entity_to_feature_table[
            tuple(sorted(feature_table.primary_entity_ids))
        ] = feature_table

    assert set(primary_entity_to_feature_table.keys()) == {
        (),
        (user_entity.id,),
        (customer_entity.id,),
        (product_action_entity.id,),
        (status_entity.id,),
    }

    await check_feature_tables_populated(session, primary_entity_to_feature_table.values())

    await check_feast_registry(app_container)

    check_online_features(deployed_feature_list, config)

    # Check offline store table for user entity
    service = app_container.feature_materialize_service
    feature_table_model = primary_entity_to_feature_table[(user_entity.id,)]
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    expected = [
        "__feature_timestamp",
        "üser id",
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100",
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h",
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h",
        "__EXTERNAL_FS_COSINE_SIMILARITY__part0",
        "__EXTERNAL_FS_COSINE_SIMILARITY_VEC__part0",
        "__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE__part0",
    ]
    assert set(df.columns.tolist()) == set(expected)
    assert df.shape[0] == 18
    assert df["__feature_timestamp"].nunique() == 2
    assert df["üser id"].isnull().sum() == 0

    # Materialize one more time
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    assert df.shape[0] == 27
    assert df["__feature_timestamp"].nunique() == 3
    assert df["üser id"].isnull().sum() == 0

    # Simulate a scheduled task
    task_payload = ScheduledFeatureMaterializeTaskPayload(
        catalog_id=feature_table_model.catalog_id,
        offline_store_feature_table_name=feature_table_model.name,
        offline_store_feature_table_id=feature_table_model.id,
    )
    await app_container.task_manager.submit(task_payload)
    df = await session.execute_query(f'SELECT * FROM "{feature_table_model.name}"')
    assert df.shape[0] == 36
    assert df["__feature_timestamp"].nunique() == 4
    assert df["üser id"].isnull().sum() == 0
