"""
Tests for feature materialization service
"""

# pylint: disable=too-many-lines
import json
import os
import textwrap
import time
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
import redis
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.common.model_util import get_version
from featurebyte.enum import InternalName, SourceType
from featurebyte.feast.patch import augment_response_with_on_demand_transforms
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.entity import DUMMY_ENTITY_COLUMN_NAME, DUMMY_ENTITY_VALUE
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.worker import get_celery
from tests.integration.conftest import (
    tag_entities_for_event_table,
    tag_entities_for_item_table,
    tag_entities_for_scd_table,
)
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY
from tests.util.helper import assert_dict_approx_equal

logger = get_logger(__name__)


@pytest.fixture(name="always_enable_feast_integration", scope="module", autouse=True)
def always_enable_feast_integration_fixture():
    """
    Enable feast integration for all tests in this module
    """
    with patch.dict(
        os.environ,
        {"FEATUREBYTE_FEAST_INTEGRATION_ENABLED": "True", "FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000"},
    ):
        yield


@pytest.fixture(name="always_patch_app_get_storage", scope="module", autouse=True)
def always_patch_app_get_storage_fixture(storage):
    """
    Patch app.get_storage for all tests in this module
    """
    with patch("featurebyte.app.get_storage", return_value=storage):
        yield


@pytest.fixture(name="app_container", scope="module")
def app_container_fixture(persistent, user, catalog, storage):
    """
    Return an app container used in tests
    """
    instance_map = {
        "user": user,
        "persistent": persistent,
        "celery": get_celery(),
        "storage": storage,
        "catalog_id": catalog.id,
        "redis": redis.from_url(REDIS_URI),
        "redis_uri": REDIS_URI,
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
    _ = event_view.groupby("ÜSER ID").aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["666h"],
        feature_names=["EXTERNAL_FS_FEATURE_NOT_DEPLOYED"],
    )["EXTERNAL_FS_FEATURE_NOT_DEPLOYED"]

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

    # feature_10 is a lookup feature by "User". feature_11 is as-at feature by "User Status". "User
    # Status" is a parent entity of "User", so the primary entity of feature_13 is "User".
    feature_13 = feature_10 + "_" + feature_11.astype(str)
    assert feature_13.primary_entity_ids == feature_10.primary_entity_ids
    feature_13.name = "Complex Feature by User"

    feature_14 = filtered_event_view.groupby(
        ["ÜSER ID", "PRODUCT_ACTION"],
    ).aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24d"],
        feature_names=["Amount Sum by Customer x Product Action 24d"],
    )["Amount Sum by Customer x Product Action 24d"]

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
            feature_13,
            feature_14,
        ]
        if feature is not None
    ]
    for feature in features:
        feature.save(conflict_resolution="retrieve")
        feature.update_readiness("PRODUCTION_READY")

    return features


@pytest_asyncio.fixture(name="saved_feature_list", scope="module")
def saved_feature_list_fixture(features):
    """
    Fixture for a saved feature list
    """
    feature_list = fb.FeatureList(features, name="EXTERNAL_FS_FEATURE_LIST")
    feature_list.save()
    yield feature_list


@pytest_asyncio.fixture(name="saved_feature_list_composite_entities", scope="module")
async def saved_feature_list_composite_entities_fixture(features):
    """
    Fixture for saved feature list with composite entities feature
    """
    features = [
        feature
        for feature in features
        if feature.name == "Amount Sum by Customer x Product Action 24d"
    ]
    feature_list = fb.FeatureList(features, name="EXTERNAL_FS_FEATURE_LIST_COMPOSITE_ENTITIES")
    feature_list.save()
    yield feature_list


@pytest.fixture(name="removed_relationships", scope="module")
def removed_relationships_fixture(
    saved_feature_list,
    saved_feature_list_composite_entities,
    event_table,
    item_table,
    scd_table,
):
    """
    Remove relationships by untagged entities after saving feature list. Everything should still
    work because of frozen relationships.

    Relationships to be removed:

    - item_id -> order_id (item table)
    - order_id -> cust_id (event table)
    - order_id -> PRODUCT_ACTION (event table)
    - order_id -> üser id (event table)
    - üser id -> user_status  (scd table)
    """
    _ = saved_feature_list
    _ = saved_feature_list_composite_entities

    def untag_entities(table):
        for column_info in table.columns_info:
            if column_info.entity_id is not None:
                table[column_info.name].as_entity(None)

    untag_entities(event_table)
    untag_entities(item_table)
    untag_entities(scd_table)
    yield

    # Retag entities to restore the relationships for other tests
    tag_entities_for_event_table(event_table)
    tag_entities_for_item_table(item_table)
    tag_entities_for_scd_table(scd_table)


@pytest_asyncio.fixture(name="deployed_feature_list", scope="module")
async def deployed_features_list_fixture(
    session, saved_feature_list, removed_relationships, app_container
):
    """
    Fixture for deployed feature list
    """
    _ = removed_relationships

    deploy_service = app_container.deploy_service
    with patch(
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=pd.Timestamp("2001-01-02 12:00:00").to_pydatetime(),
    ):
        deployment = saved_feature_list.deploy()
        with patch(
            "featurebyte.service.feature_materialize.datetime", autospec=True
        ) as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
            await deploy_service.update_deployment(
                deployment_id=deployment.id,
                to_enable_deployment=True,
            )

    # check that the feature list's feast_enabled attribute is set to True
    feature_list_model = await app_container.feature_list_service.get_document(
        saved_feature_list.id, populate_remote_attributes=False
    )
    assert feature_list_model.store_info.feast_enabled

    yield deployment
    await deploy_service.update_deployment(
        deployment_id=deployment.id,
        to_enable_deployment=False,
    )

    if session.source_type == SourceType.DATABRICKS_UNITY:
        # check that on demand feature udf is dropped
        df = await session.execute_query(
            sql_to_string(parse_one("SHOW USER FUNCTIONS"), session.source_type)
        )
        all_udfs = set(df.function.apply(lambda x: x.split(".")[-1]).to_list())
        udfs_for_on_demand_func = [udf for udf in all_udfs if udf.startswith("udf_")]
        assert len(udfs_for_on_demand_func) == 0


@pytest_asyncio.fixture(name="deployed_feature_list_composite_entities", scope="module")
async def deployed_features_list_composite_entities_fixture(
    saved_feature_list_composite_entities, app_container
):
    """
    Fixture for deployed feature list
    """
    feature_list = saved_feature_list_composite_entities

    deploy_service = app_container.deploy_service
    with patch(
        "featurebyte.service.feature_manager.get_next_job_datetime",
        return_value=pd.Timestamp("2001-01-02 12:00:00").to_pydatetime(),
    ):
        deployment = feature_list.deploy()
        with patch(
            "featurebyte.service.feature_materialize.datetime", autospec=True
        ) as mock_datetime:
            mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
            await deploy_service.update_deployment(
                deployment_id=deployment.id,
                to_enable_deployment=True,
            )

    yield deployment
    await deploy_service.update_deployment(
        deployment_id=deployment.id,
        to_enable_deployment=False,
    )


@pytest_asyncio.fixture(name="offline_store_feature_tables", scope="module")
async def offline_store_feature_tables_fixture(app_container, deployed_feature_list):
    """
    Fixture for offline store feature tables based on the deployed features
    """
    _ = deployed_feature_list
    primary_entity_to_feature_table = {}
    async for (
        feature_table
    ) in app_container.offline_store_feature_table_service.list_documents_iterator(
        query_filter={},
    ):
        primary_entity_to_feature_table[feature_table.name] = feature_table
    return primary_entity_to_feature_table


@pytest.fixture(name="user_entity_ttl_feature_table")
def user_entity_ttl_feature_table_fixture(offline_store_feature_tables):
    """
    Return the user entity feature table
    """
    return offline_store_feature_tables["cat1_userid_1h"]


@pytest.fixture(name="user_entity_non_ttl_feature_table")
def user_entity_non_ttl_feature_table_fixture(offline_store_feature_tables):
    """
    Return the user entity feature table
    """
    return offline_store_feature_tables["cat1_userid_1d"]


@pytest_asyncio.fixture(name="expected_entity_lookup_feature_table_names")
async def expected_entity_lookup_feature_table_names_fixture(
    app_container,
    deployed_feature_list,
    order_entity,
    product_action_entity,
    customer_entity,
    user_entity,
    status_entity,
):
    """
    Fixture for expected entity lookup feature table names
    """
    expected = []
    feature_list_model = await app_container.feature_list_service.get_document(
        deployed_feature_list.feature_list_id
    )

    def _get_relationship_info(child_entity, parent_entity):
        for info in feature_list_model.relationships_info:
            if info.entity_id == child_entity.id and info.related_entity_id == parent_entity.id:
                return info
        raise AssertionError(
            f"Relationship with child as {child_entity.name} and parent as {parent_entity.name} not found"
        )

    for info in [
        _get_relationship_info(order_entity, customer_entity),
        _get_relationship_info(order_entity, product_action_entity),
        _get_relationship_info(order_entity, user_entity),
        _get_relationship_info(user_entity, status_entity),
    ]:
        expected.append(f"fb_entity_lookup_{info.id}")

    return set(expected)


@pytest.fixture(name="expected_feature_table_names")
def expected_feature_table_names_fixture(expected_entity_lookup_feature_table_names):
    """
    Fixture for expected feature table names
    """
    expected = {
        "cat1__no_entity_1h",
        "cat1_product_action_1h",
        "cat1_cust_id_1h",
        "cat1_userid_1h",
        "cat1_userid_1d",
        "cat1_user_status_1d",
        "cat1_order_id_1d",
        "cat1_userid_product_action_1h",
    }
    expected.update(expected_entity_lookup_feature_table_names)
    return expected


@pytest.fixture(name="expected_feature_service_names")
def expected_feature_service_names_fixture():
    """
    Fixture for expected feature service names
    """
    return {
        f"EXTERNAL_FS_FEATURE_LIST_{get_version()}",
        f"EXTERNAL_FS_FEATURE_LIST_COMPOSITE_ENTITIES_{get_version()}",
    }


@pytest.mark.order(1)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_feature_tables_expected(
    offline_store_feature_tables,
    expected_feature_table_names,
):
    """
    Test offline store feature tables are created as expected
    """
    assert set(offline_store_feature_tables.keys()) == expected_feature_table_names


@pytest.mark.order(1)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_feature_cluster_with_expected_internal_relationships(offline_store_feature_tables):
    """
    Check that feature level relationship information are included in feature cluster correctly

    Table cat1_userid_1d is one such table. It should include non-empty relationships info because
    the complex feature "Complex Feature by User" requires parent-child relationship within the
    feature table.
    """
    feature_tables_with_internal_relationships = {}
    for name, feature_table_model in offline_store_feature_tables.items():
        if feature_table_model.feature_cluster.feature_node_relationships_infos is None:
            continue
        for info in feature_table_model.feature_cluster.feature_node_relationships_infos:
            if info.relationships_info:
                feature_tables_with_internal_relationships[name] = feature_table_model
                break
    feature_table_names = list(feature_tables_with_internal_relationships.keys())
    assert feature_table_names == ["cat1_userid_1d"]


@pytest.mark.order(2)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
async def test_feature_tables_populated(session, offline_store_feature_tables, source_type):
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
        expected = set(
            [InternalName.FEATURE_TIMESTAMP_COLUMN.value]
            + feature_table.serving_names
            + feature_table.output_column_names
        )
        if len(feature_table.serving_names) > 0:
            expected.add(" x ".join(feature_table.serving_names))
        elif source_type == SourceType.DATABRICKS_UNITY:
            expected.add(DUMMY_ENTITY_COLUMN_NAME)
        assert set(df.columns.tolist()) == expected

        if len(feature_table.serving_names) == 0 and source_type == SourceType.DATABRICKS_UNITY:
            assert (df[DUMMY_ENTITY_COLUMN_NAME] == DUMMY_ENTITY_VALUE).all()


@pytest.mark.order(3)
@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_databricks_udf_created(session, offline_store_feature_tables, source_type):
    """Test that udf is created in databricks"""
    _ = offline_store_feature_tables
    df = await session.execute_query(
        sql_to_string(parse_one("SHOW USER FUNCTIONS"), session.source_type)
    )
    all_udfs = set(df.function.apply(lambda x: x.split(".")[-1]).to_list())
    assert len(all_udfs) > 0
    udfs_for_on_demand_func = [udf for udf in all_udfs if udf.startswith("udf_")]
    if source_type == SourceType.DATABRICKS_UNITY:
        assert len(udfs_for_on_demand_func) == 2
    else:
        assert len(udfs_for_on_demand_func) == 0


@pytest.mark.order(4)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
@pytest.mark.asyncio
@pytest.mark.usefixtures("deployed_feature_list", "deployed_feature_list_composite_entities")
async def test_feast_registry(
    app_container, expected_feature_table_names, expected_feature_service_names, source_type
):
    """
    Check feast registry is populated correctly
    """
    feast_registry = await app_container.feast_registry_service.get_feast_registry_for_catalog()
    assert feast_registry is not None
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )

    # Check feature views and feature services
    feature_service_name = f"EXTERNAL_FS_FEATURE_LIST_{get_version()}"
    assert {fv.name for fv in feature_store.list_feature_views()} == expected_feature_table_names
    assert {
        fs.name for fs in feature_store.list_feature_services()
    } == expected_feature_service_names

    # Check feast materialize and get_online_features
    feature_store = await app_container.feast_feature_store_service.get_feast_feature_store(
        feast_registry.id
    )
    feature_service = feature_store.get_feature_service(feature_service_name)
    version = get_version()
    entity_row = {
        "üser id": 5,
        "cust_id": 761,
        "user_status": "STÀTUS_CODE_37",
        "PRODUCT_ACTION": "detail",
        "order_id": "T1230",
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 12:00:00"),
        "üser id x PRODUCT_ACTION": "detail::761",
    }
    with patch.object(
        feature_store,
        "_augment_response_with_on_demand_transforms",
        new=augment_response_with_on_demand_transforms,
    ):
        online_features = feature_store.get_online_features(
            features=feature_service,
            entity_rows=[entity_row],
        ).to_dict()

    online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"] = [
        (
            json.loads(online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"][0])
            if online_features[f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}"][0]
            else None
        )
    ]
    if source_type != SourceType.DATABRICKS_UNITY:
        online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"] = [
            (
                json.loads(online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"][0])
                if online_features[f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}"][0]
                else None
            )
        ]
    expected = {
        f"Amount Sum by Customer x Product Action 24d_{version}": [None],
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
        f"Complex Feature by User_{version}": ["STÀTUS_CODE_37_1"],
        "cust_id": ["761"],
        "order_id": ["T1230"],
        "user_status": ["STÀTUS_CODE_37"],
        "üser id": ["5"],
        "üser id x PRODUCT_ACTION": ["detail::761"],
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop(f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}")
        expected.pop(f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}")
    assert_dict_approx_equal(online_features, expected)

    # set point in time > 2001-01-02 12:00:00 +  2 hours (frequency is 1 hour) &
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 14:00:01")
    with patch.object(
        feature_store,
        "_augment_response_with_on_demand_transforms",
        new=augment_response_with_on_demand_transforms,
    ):
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
        "üser id x PRODUCT_ACTION": ["detail::761"],
        f"Amount Sum by Customer x Product Action 24d_{version}": [None],
        f"User Status Feature_{version}": ["STÀTUS_CODE_37"],
        f"Current Number of Users With This Status_{version}": [1],
        f"Complex Feature by User_{version}": ["STÀTUS_CODE_37_1"],
        f"Most Frequent Item Type by Order_{version}": ["type_12"],
        f"EXTERNAL_FS_COUNT_OVERALL_7d_{version}": [None],
        f"EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_{version}": [None],
        f"EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}": [None],
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}": [None],
        f"EXTERNAL_FS_COSINE_SIMILARITY_{version}": [None],
        f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}": [None],
        # due to implementation in _get_vector_cosine_similarity_function_name,
        # any null value in the input vector will result in 0 cosine similarity
        f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}": [0.0],
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop(f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}")
        expected.pop(f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}")
    assert_dict_approx_equal(online_features, expected)

    # set the point in time earlier than the 2001-01-02 12:00:00
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 11:59:59")
    with patch.object(
        feature_store,
        "_augment_response_with_on_demand_transforms",
        new=augment_response_with_on_demand_transforms,
    ):
        online_features = feature_store.get_online_features(
            features=feature_service,
            entity_rows=[entity_row],
        ).to_dict()
    assert_dict_approx_equal(online_features, expected)


@pytest.mark.order(5)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__all_entities_provided(config, deployed_feature_list, source_type):
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
    if source_type != SourceType.DATABRICKS_UNITY:
        feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = json.loads(
            feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"]
        )
    expected = {
        "Amount Sum by Customer x Product Action 24d": 254.23000000000002,
        "Complex Feature by User": "STÀTUS_CODE_37_1",
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
        "cust_id": 761,
        "order_id": "T1230",
        "user_status": "STÀTUS_CODE_37",
        "üser id": 5,
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop("EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h")
        expected.pop("EXTERNAL_FS_COSINE_SIMILARITY_VEC")
    assert_dict_approx_equal(feat_dict, expected)


@pytest.fixture
def expected_features_order_id_T3850(source_type):
    """
    Expected features for entity order_id T3850
    """
    expected = {
        "Amount Sum by Customer x Product Action 24d": 169.76999999999998,
        "Complex Feature by User": "STÀTUS_CODE_26_1",
        "Current Number of Users With This Status": 1,
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": {
            "__MISSING__": 174.39,
            "detail": 169.77,
            "purchase": 473.31,
            "rëmove": 102.37,
            "àdd": 27.1,
        },
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": 623.15,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100": 62315.0,
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            0.5365338952415342,
            0.4908373917748641,
            0.42408493050514906,
            0.5512648363475837,
            0.5269536439690168,
            0.4490616290417774,
            0.41383692828120616,
            0.4543201999832706,
            0.5041296835615284,
            0.43798778166578617,
        ],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": 667.0592974268257,
        "EXTERNAL_FS_COSINE_SIMILARITY": 0.0,
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": 0.9171356659119657,
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": 43,
        "EXTERNAL_FS_COUNT_OVERALL_7d": 149,
        "Most Frequent Item Type by Order": "type_24",
        "User Status Feature": "STÀTUS_CODE_26",
        "order_id": "T3850",
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop("EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h")
        expected.pop("EXTERNAL_FS_COSINE_SIMILARITY_VEC")
    return expected


def process_output_features_helper(feat_dict, source_type):
    """
    Process output features to facilitate testing
    """
    feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"] = json.loads(
        feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"]
    )
    if source_type != SourceType.DATABRICKS_UNITY:
        feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = json.loads(
            feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"]
        )


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__primary_entity_ids(
    config, deployed_feature_list, expected_features_order_id_T3850, source_type
):
    """
    Check online features by providing only the primary entity ids. Expect the online serving
    service to lookup parent entities.

    List of lookups to be made:

    - order_id:T3850 -> cust_id:594
    - order_id:T3850 -> PRODUCT_ACTION:detail
    - order_id:T3850 -> üser id:7
    - üser id:7 -> user_status:STÀTUS_CODE_26
    """
    client = config.get_client()
    deployment = deployed_feature_list

    entity_serving_names = [
        {
            "order_id": "T3850",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    tic = time.time()
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
    process_output_features_helper(feat_dict, source_type)
    assert_dict_approx_equal(feat_dict, expected_features_order_id_T3850)


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__invalid_child_entity(config, deployed_feature_list):
    """
    Check online features using a child entity that is a feature list's supported_serving_entity_ids
    but not enabled_serving_entity_ids
    """
    client = config.get_client()
    deployment = deployed_feature_list

    entity_serving_names = [
        {
            "item_id": "ITEM_42",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)
    res = client.post(
        f"/deployment/{deployment.id}/online_features",
        json=data.json_dict(),
    )
    assert res.status_code == 422
    assert res.json() == {
        "detail": 'Required entities are not provided in the request: Order (serving name: "order_id")'
    }


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__non_existing_order_id(
    config, deployed_feature_list, expected_features_order_id_T3850, source_type
):
    """
    Test online features with a mix of existing and non-existing serving entities
    """
    client = config.get_client()
    deployment = deployed_feature_list

    entity_serving_name = {"order_id": "T3850"}
    entity_serving_name_non_exist = {"order_id": "non_existing_order_id"}
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        data = OnlineFeaturesRequestPayload(
            entity_serving_names=[entity_serving_name, entity_serving_name_non_exist]
        )
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    assert res.status_code == 200
    features = res.json()["features"]
    process_output_features_helper(features[0], source_type)
    assert_dict_approx_equal(features[0], expected_features_order_id_T3850)
    expected_non_existing_order_id_features = entity_serving_name_non_exist.copy()
    expected_non_existing_order_id_features.update(
        {
            feature_name: None
            for feature_name in expected_features_order_id_T3850
            if feature_name != "order_id"
        }
    )
    expected_non_existing_order_id_features["EXTERNAL_FS_COUNT_OVERALL_7d"] = 149
    if source_type != SourceType.DATABRICKS_UNITY:
        expected_non_existing_order_id_features["EXTERNAL_FS_COSINE_SIMILARITY_VEC"] = 0
    assert_dict_approx_equal(features[0], expected_features_order_id_T3850)
    assert_dict_approx_equal(features[1], expected_non_existing_order_id_features)


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__composite_entities(config, deployed_feature_list_composite_entities):
    """
    Check online features when the feature list only has a feature with composite entities
    """
    client = config.get_client()
    deployment = deployed_feature_list_composite_entities

    entity_serving_names = [
        {
            "order_id": "T3850",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    tic = time.time()
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
    expected = {
        "Amount Sum by Customer x Product Action 24d": 169.76999999999998,
        "order_id": "T3850",
    }
    assert_dict_approx_equal(feat_dict, expected)


@pytest.mark.order(7)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
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
    if source_type != SourceType.DATABRICKS_UNITY:
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


@pytest.mark.order(8)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
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
        f"Complex Feature by User_{version}",
    ]
    assert df_0.shape[0] == 9
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
    assert df_1.shape[0] == 18
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


@pytest.mark.order(9)
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


@pytest.mark.order(10)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY, indirect=True)
def test_online_features__patch_feast(config, deployed_feature_list):
    """Test feast online features with patching works without error"""
    client = config.get_client()
    deployment = deployed_feature_list

    entity_serving_name = {
        "üser id": 5,
        "cust_id": 761,
        "PRODUCT_ACTION": "detail",
        "user_status": "STÀTUS_CODE_37",
        "order_id": "T1230",
    }
    entity_serving_name_non_exist = {
        "üser id": "non_existing_user_id",
        "cust_id": 123456,
        "PRODUCT_ACTION": "non_existing_product_action",
        "user_status": "non_existing_user_status",
        "order_id": "non_existing_order_id",
    }
    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)

        data = OnlineFeaturesRequestPayload(entity_serving_names=[entity_serving_name])
        res1 = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        ).json()

        data = OnlineFeaturesRequestPayload(entity_serving_names=[entity_serving_name_non_exist])
        res2 = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        ).json()

        data = OnlineFeaturesRequestPayload(
            entity_serving_names=[entity_serving_name_non_exist, entity_serving_name]
        )
        res3 = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        ).json()
        assert res3 == {"features": res2["features"] + res1["features"]}
