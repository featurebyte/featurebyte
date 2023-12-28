"""
Tests for feature materialization service
"""
import json
import os
import textwrap
import time
from datetime import datetime
from unittest.mock import patch

import freezegun
import pandas as pd
import pytest
import pytest_asyncio
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import sql_to_string
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
def features_fixture(event_table, scd_table, source_type):  # pylint: disable=too-many-locals
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

    if source_type != SourceType.DATABRICKS_UNITY:
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
    else:
        feature_8 = None
        feature_9 = None

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
        feature
        for feature in [
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
        if feature is not None
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
        with patch(
            "featurebyte.service.feature_materialize.datetime", autospec=True
        ) as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
            deployment.enable()

    yield deployment
    deployment.disable()


@pytest_asyncio.fixture(name="offline_store_feature_tables", scope="module")
async def offline_store_feature_tables_fixture(app_container, deployed_feature_list):
    """
    Fixture for offline store feature tables based on the deployed features
    """
    _ = deployed_feature_list
    primary_entity_to_feature_table = {}
    async for feature_table in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={},
    ):
        primary_entity_to_feature_table[
            tuple(sorted(feature_table.primary_entity_ids))
        ] = feature_table
    return primary_entity_to_feature_table


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
def test_feature_tables_expected(
    offline_store_feature_tables,
    user_entity,
    customer_entity,
    product_action_entity,
    status_entity,
):
    """
    Test offline store feature tables are created as expected
    """
    assert set(offline_store_feature_tables.keys()) == {
        (),
        (user_entity.id,),
        (customer_entity.id,),
        (product_action_entity.id,),
        (status_entity.id,),
    }


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_feature_tables_populated(session, offline_store_feature_tables):
    """
    Check feature tables are populated correctly
    """
    for feature_table in offline_store_feature_tables.values():
        df = await session.execute_query(
            sql_to_string(
                parse_one(f'SELECT * FROM "{feature_table.name}"'),
                session.source_type,
            )
        )

        # Should not be empty
        assert df.shape[0] > 0

        # Should have all the serving names and output columns tracked in OfflineStoreFeatureTable
        assert set(df.columns.tolist()) == set(
            [InternalName.FEATURE_TIMESTAMP_COLUMN.value]
            + feature_table.serving_names
            + feature_table.output_column_names
        )


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_feast_registry(app_container):
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
        "fb_entity_userid_fjs_3600_1800_1800_ttl",
        "fb_entity_userid_fjs_86400_0_0",
        "fb_entity_user_status_fjs_86400_0_0",
    }
    assert {fs.name for fs in feature_store.list_feature_services()} == {"EXTERNAL_FS_FEATURE_LIST"}

    # Check feast materialize and get_online_features
    feature_service = feature_store.get_feature_service("EXTERNAL_FS_FEATURE_LIST")
    entity_row = {
        "üser id": 5,
        "cust_id": 761,
        "user_status": "STÀTUS_CODE_37",
        "PRODUCT_ACTION": "detail",
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 12:00:00"),
    }
    online_features = feature_store.get_online_features(
        features=feature_service,
        entity_rows=[entity_row],
    ).to_dict()
    online_features["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"] = [
        json.loads(online_features["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"][0])
        if online_features["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"][0]
        else None
    ]
    online_features["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = [
        json.loads(online_features["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"][0])
        if online_features["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"][0]
        else None
    ]
    expected = {
        "Current Number of Users With This Status": [1.0],
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": [
            {
                "__MISSING__": 240.76,
                "detail": 254.23,
                "purchase": 216.87,
                "rëmove": 98.34,
                "àdd": 146.22,
            }
        ],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": [683.55],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": [68355.0],
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            [
                0.6807108071969569,
                0.41463276624595335,
                0.4432548634609973,
                0.6828628340472915,
                0.5967769997569004,
                0.5210525989755145,
                0.4687023396052305,
                0.35638918237609585,
                0.42879787416376725,
                0.548615163058392,
            ]
        ],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": [727.4592974268256],
        "EXTERNAL_FS_COSINE_SIMILARITY": [0.0],
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": [0.8578220571057548],
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": [43.0],
        "EXTERNAL_FS_COUNT_OVERALL_7d": [149.0],
        "PRODUCT_ACTION": ["detail"],
        "User Status Feature": ["STÀTUS_CODE_37"],
        "cust_id": ["761"],
        "user_status": ["STÀTUS_CODE_37"],
        "üser id": ["5"],
    }
    assert_dict_approx_equal(online_features, expected)

    # set point in time > 2001-01-02 12:00:00 +  2 hours (frequency is 1 hour) &
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 14:00:01")
    online_features = feature_store.get_online_features(
        features=feature_service,
        entity_rows=[entity_row],
    ).to_dict()
    expected = {
        "üser id": ["5"],
        "cust_id": ["761"],
        "PRODUCT_ACTION": ["detail"],
        "user_status": ["STÀTUS_CODE_37"],
        "User Status Feature": ["STÀTUS_CODE_37"],
        "Current Number of Users With This Status": [1],
        "EXTERNAL_FS_COUNT_OVERALL_7d": [None],
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": [None],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": [None],
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": [None],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": [None],
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": [None],
        "EXTERNAL_FS_COSINE_SIMILARITY": [None],
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [None],
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": [None],
    }
    assert_dict_approx_equal(online_features, expected)

    # set the point in time earlier than the 2001-01-02 12:00:00
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 11:59:59")
    online_features = feature_store.get_online_features(
        features=feature_service,
        entity_rows=[entity_row],
    ).to_dict()
    assert_dict_approx_equal(online_features, expected)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@freezegun.freeze_time("2001-01-02 12:00:00")
def test_online_features(config, deployed_feature_list):
    """
    Check online features are populated correctly
    """
    client = config.get_client()
    deployment = deployed_feature_list

    entity_serving_names = [
        {
            "üser id": 5,
            "cust_id": 761,
            "PRODUCT_ACTION": "detail",
            # Note: shouldn't have to provide this once parent entity lookup is supported (via child
            # entity üser id)
            "user_status": "STÀTUS_CODE_37",
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
        "Current Number of Users With This Status": 1.0,
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": {
            "__MISSING__": 240.76,
            "detail": 254.23,
            "purchase": 216.87,
            "rëmove": 98.34,
            "àdd": 146.22,
        },
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": 683.55,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": 68355.0,
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            0.6807108071969569,
            0.41463276624595335,
            0.4432548634609973,
            0.6828628340472915,
            0.5967769997569004,
            0.5210525989755145,
            0.4687023396052305,
            0.35638918237609585,
            0.42879787416376725,
            0.548615163058392,
        ],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": 727.4592974268256,
        "EXTERNAL_FS_COSINE_SIMILARITY": 0.0,
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": 0.8578220571057548,
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": 43.0,
        "EXTERNAL_FS_COUNT_OVERALL_7d": 149.0,
        "PRODUCT_ACTION": "detail",
        "User Status Feature": "STÀTUS_CODE_37",
        "cust_id": "761",
        "user_status": "STÀTUS_CODE_37",
        "üser id": "5",
    }
    assert_dict_approx_equal(feat_dict, expected)


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_simulated_materialize(
    app_container,
    session,
    user_entity,
    offline_store_feature_tables,
    source_type,
):
    """
    Test simulating scheduled feature materialization
    """
    # Check calling scheduled_materialize_features()
    service = app_container.feature_materialize_service
    feature_table_model = offline_store_feature_tables[(user_entity.id,)]
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'), session.source_type
        ),
    )
    if source_type == SourceType.SNOWFLAKE:
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
    else:
        expected = [
            "__feature_timestamp",
            "üser id",
            "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d",
            "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100",
            "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h",
            "__EXTERNAL_FS_COSINE_SIMILARITY__part0",
            "__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE__part0",
        ]

    assert set(df.columns.tolist()) == set(expected)
    assert df.shape[0] == 18
    assert df["__feature_timestamp"].nunique() == 2
    assert df["üser id"].isnull().sum() == 0

    # Materialize one more time
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'),
            session.source_type,
        )
    )
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
    df = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'),
            session.source_type,
        )
    )
    assert df.shape[0] == 36
    assert df["__feature_timestamp"].nunique() == 4
    assert df["üser id"].isnull().sum() == 0


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_feature_tables_have_primary_key_constraints(session, offline_store_feature_tables):
    """
    Check feature tables have primary key constraints in Databricks
    """
    for feature_table in offline_store_feature_tables.values():
        df = await session.execute_query(
            textwrap.dedent(
                f"""
                SELECT *
                FROM information_schema.constraint_table_usage
                WHERE table_schema ILIKE '{session.schema_name}'
                AND table_name = '{feature_table.name}'
                """
            ).strip()
        )
        assert df.shape[0] == 1
