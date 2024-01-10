"""
Tests for feature materialization service
"""
import json
import os
import textwrap
import time
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.common.model_util import get_version
from featurebyte.enum import InternalName, SourceType
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from featurebyte.storage import LocalTempStorage
from featurebyte.worker import get_celery
from tests.util.helper import assert_dict_approx_equal

logger = get_logger(__name__)


@pytest.fixture(name="always_enable_feast_integration", scope="module", autouse=True)
def always_enable_feast_integration_fixture():
    """
    Enable feast integration for all tests in this module
    """
    with patch.dict(os.environ, {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True"}):
        yield


@pytest.fixture(name="app_container", scope="module")
def app_container_fixture(persistent, user, catalog):
    """
    Return an app container used in tests
    """
    instance_map = {
        "user": user,
        "persistent": persistent,
        "celery": get_celery(),
        "storage": LocalTempStorage(),
        "catalog_id": catalog.id,
    }
    return LazyAppContainer(app_container_config=app_container_config, instance_map=instance_map)


@pytest.fixture(name="features", scope="module")
def features_fixture(
    event_table, scd_table, source_type, item_table
):  # pylint: disable=too-many-locals
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
    feature_11 = scd_view.groupby("User Status").aggregate_asat(
        method="count", feature_name="Current Number of Users With This Status"
    )
    feature_11.name = "Current Number of Users With This Status"

    item_view = item_table.get_view()
    item_type_counts = item_view.groupby("order_id", category="item_type").aggregate(
        method="count", feature_name="my_item_feature"
    )
    feature_12 = item_type_counts.cd.most_frequent()
    feature_12.name = "Most Frequent Item Type by Order"

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
            feature_12,
        ]
        if feature is not None
    ]
    for feature in features:
        feature.save()
        feature.update_readiness("PRODUCTION_READY")

    return features


@pytest_asyncio.fixture(name="deployed_feature_list", scope="module")
async def deployed_features_list_fixture(session, features, expected_udf_names):
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

    if session.source_type == SourceType.DATABRICKS_UNITY:
        # check that on demand feature udf is dropped
        df = await session.execute_query(
            sql_to_string(parse_one("SHOW USER FUNCTIONS"), session.source_type)
        )
        all_udfs = set(df.function.apply(lambda x: x.split(".")[-1]).to_list())
        assert all_udfs.intersection(set(expected_udf_names)) == set()


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
        primary_entity_to_feature_table[feature_table.name] = feature_table
    return primary_entity_to_feature_table


@pytest.fixture(name="user_entity_ttl_feature_table")
def user_entity_ttl_feature_table_fixture(offline_store_feature_tables, app_container):
    """
    Return the user entity feature table
    """
    catalog_id = app_container.catalog_id
    return offline_store_feature_tables[f"fb_entity_userid_fjs_3600_1800_1800_ttl_{catalog_id}"]


@pytest.fixture(name="user_entity_non_ttl_feature_table")
def user_entity_non_ttl_feature_table_fixture(offline_store_feature_tables, app_container):
    """
    Return the user entity feature table
    """
    catalog_id = app_container.catalog_id
    return offline_store_feature_tables[f"fb_entity_userid_fjs_86400_0_0_{catalog_id}"]


@pytest.fixture(name="expected_feature_table_names")
def expected_feature_table_names_fixture(app_container):
    """
    Fixture for expected feature table names
    """
    catalog_id = app_container.catalog_id
    return {
        f"fb_entity_overall_fjs_3600_1800_1800_ttl_{catalog_id}",
        f"fb_entity_product_action_fjs_3600_1800_1800_ttl_{catalog_id}",
        f"fb_entity_cust_id_fjs_3600_1800_1800_ttl_{catalog_id}",
        f"fb_entity_userid_fjs_3600_1800_1800_ttl_{catalog_id}",
        f"fb_entity_userid_fjs_86400_0_0_{catalog_id}",
        f"fb_entity_user_status_fjs_86400_0_0_{catalog_id}",
        f"fb_entity_order_id_fjs_86400_0_0_{catalog_id}",
    }


@pytest.fixture(name="expected_udf_names", scope="module")
def expected_udf_names_fixture(features):
    """
    Fixture for expected udf names
    """
    return [
        feature.cached_model.offline_store_info.udf_info.sql_function_name
        for feature in features
        if feature.cached_model.offline_store_info.udf_info
    ]


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
def test_feature_tables_expected(
    offline_store_feature_tables,
    expected_feature_table_names,
):
    """
    Test offline store feature tables are created as expected
    """
    assert set(offline_store_feature_tables.keys()) == expected_feature_table_names


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


@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_databricks_udf_created(session, offline_store_feature_tables, expected_udf_names):
    """Test that udf is created in databricks"""
    # extract expected udf names from features
    df = await session.execute_query(
        sql_to_string(parse_one("SHOW USER FUNCTIONS"), session.source_type)
    )
    all_udfs = set(df.function.apply(lambda x: x.split(".")[-1]).to_list())
    assert len(all_udfs) > 0
    assert set(expected_udf_names).issubset(all_udfs)


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
@pytest.mark.usefixtures("deployed_feature_list")
async def test_feast_registry(app_container, expected_feature_table_names):
    """
    Check feast registry is populated correctly
    """
    feast_registry = await app_container.feast_registry_service.get_feast_registry_for_catalog()
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )

    # Check feature views and feature services
    assert {fv.name for fv in feature_store.list_feature_views()} == expected_feature_table_names
    assert {fs.name for fs in feature_store.list_feature_services()} == {"EXTERNAL_FS_FEATURE_LIST"}

    # Check feast materialize and get_online_features
    feature_service = feature_store.get_feature_service("EXTERNAL_FS_FEATURE_LIST")
    version = get_version()
    entity_row = {
        "üser id": 5,
        "cust_id": 761,
        "user_status": "STÀTUS_CODE_37",
        "PRODUCT_ACTION": "detail",
        "order_id": "T1230",
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 12:00:00"),
    }
    online_features = feature_store.get_online_features(
        features=feature_service,
        entity_rows=[entity_row],
    ).to_dict()
    online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"] = [
        json.loads(online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"][0])
        if online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"][0]
        else None
    ]
    online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"] = [
        json.loads(online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"][0])
        if online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"][0]
        else None
    ]
    expected = {
        f"Current Number of Users With This Status_{version}": [1.0],
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}": [
            {
                "__MISSING__": 240.76,
                "detail": 254.23,
                "purchase": 216.87,
                "rëmove": 98.34,
                "àdd": 146.22,
            }
        ],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}": [683.55],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_{version}": [68355.0],
        f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}": [
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
        f"EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}": [727.4592974268256],
        f"EXTERNAL_FS_COSINE_SIMILARITY_{version}": [0.0],
        f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}": [0.8578220571057548],
        f"EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d_{version}": [43.0],
        f"EXTERNAL_FS_COUNT_OVERALL_7d_{version}": [149.0],
        f"Most Frequent Item Type by Order_{version}": ["type_12"],
        "PRODUCT_ACTION": ["detail"],
        f"User Status Feature_{version}": ["STÀTUS_CODE_37"],
        "cust_id": ["761"],
        "order_id": ["T1230"],
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
        "order_id": ["T1230"],
        f"User Status Feature_{version}": ["STÀTUS_CODE_37"],
        f"Current Number of Users With This Status_{version}": [1],
        f"Most Frequent Item Type by Order_{version}": ["type_12"],
        f"EXTERNAL_FS_COUNT_OVERALL_7d_{version}": [None],
        f"EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_{version}": [None],
        f"EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}": [None],
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}": [None],
        f"EXTERNAL_FS_COSINE_SIMILARITY_{version}": [None],
        f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}": [None],
        f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}": [None],
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
            "order_id": "T1230",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    tic = time.time()
    # Note: don't mock with freezegun since it also freezes elapsed time tracking for logging
    # purpose within OnlineServingService
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
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
        "Most Frequent Item Type by Order": "type_12",
        "PRODUCT_ACTION": "detail",
        "User Status Feature": "STÀTUS_CODE_37",
        "cust_id": "761",
        "order_id": "T1230",
        "user_status": "STÀTUS_CODE_37",
        "üser id": "5",
    }
    assert_dict_approx_equal(feat_dict, expected)


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_simulated_materialize__ttl_feature_table(
    app_container,
    session,
    user_entity_ttl_feature_table,
    source_type,
):
    """
    Test simulating scheduled feature materialization for a feature table with TTL
    """
    # Check calling scheduled_materialize_features()
    service = app_container.feature_materialize_service
    feature_table_model = user_entity_ttl_feature_table
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'), session.source_type
        ),
    )
    version = get_version()
    if source_type == SourceType.SNOWFLAKE:
        expected = [
            "__feature_timestamp",
            "üser id",
            f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}",
            f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_{version}",
            f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}",
            f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}",
            f"__EXTERNAL_FS_COSINE_SIMILARITY_{version}__part0",
            f"__EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}__part0",
            f"__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}__part0",
        ]
    else:
        expected = [
            "__feature_timestamp",
            "üser id",
            f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}",
            f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_{version}",
            f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}",
            f"__EXTERNAL_FS_COSINE_SIMILARITY_{version}__part0",
            f"__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}__part0",
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


async def reload_feature_table_model(app_container, feature_table_model):
    """
    Reload feature table model from persistent
    """
    feature_table_service = app_container.offline_store_feature_table_service
    feature_table_model = await feature_table_service.get_document(feature_table_model.id)
    return feature_table_model


@pytest.mark.parametrize("source_type", ["snowflake", "databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_simulated_materialize__non_ttl_feature_table(
    app_container,
    session,
    user_entity_non_ttl_feature_table,
):
    """
    Test simulating scheduled feature materialization for a feature table without TTL
    """
    feature_table_model = user_entity_non_ttl_feature_table
    service = app_container.feature_materialize_service

    df_0 = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'), session.source_type
        ),
    )
    version = get_version()
    assert df_0.columns.tolist() == [
        "__feature_timestamp",
        "üser id",
        f"User Status Feature_{version}",
    ]
    assert df_0.shape[0] == 4
    assert df_0["__feature_timestamp"].nunique() == 1

    # Trigger a materialization task after the feature table is created. This should materialize
    # features for the entities that appear since deployment time (mocked above) till now.
    feature_table_model = await reload_feature_table_model(app_container, feature_table_model)
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df_1 = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'), session.source_type
        ),
    )
    assert df_1.shape[0] == 13
    assert df_1["__feature_timestamp"].nunique() == 2

    # Materialize one more time. Since there is no new data that appears since the last
    # materialization, the entity universe is empty. This shouldn't insert any new rows into the
    # feature table.
    feature_table_model = await reload_feature_table_model(app_container, feature_table_model)
    await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df_2 = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'),
            session.source_type,
        )
    )
    assert df_2.shape[0] == df_1.shape[0]
    assert df_2["__feature_timestamp"].nunique() == df_1["__feature_timestamp"].nunique()


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
