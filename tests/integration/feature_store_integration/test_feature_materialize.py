"""
Tests for feature materialization service
"""

import json
import os
import textwrap
import time
from datetime import datetime
from pprint import pprint
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import redis
from bson import ObjectId
from sqlglot import parse_one

import featurebyte as fb
from featurebyte.common.model_util import get_version
from featurebyte.enum import DBVarType, InternalName, SourceType
from featurebyte.feast.patch import augment_response_with_on_demand_transforms
from featurebyte.logging import get_logger
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.entity import DUMMY_ENTITY_COLUMN_NAME, DUMMY_ENTITY_VALUE
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload
from featurebyte.schema.worker.task.feature_materialize_sync import (
    FeatureMaterializeSyncTaskPayload,
)
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from featurebyte.worker import get_celery
from tests.integration.conftest import (
    TEST_REDIS_URI,
    tag_entities_for_event_table,
    tag_entities_for_item_table,
    tag_entities_for_scd_table,
)
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_dict_approx_equal

logger = get_logger(__name__)


@pytest.fixture(name="always_enable_feast_integration", scope="module", autouse=True)
def always_enable_feast_integration_fixture():
    """
    Enable feast integration for all tests in this module
    """
    with patch.dict(
        os.environ,
        {"FEATUREBYTE_GRAPH_CLEAR_PERIOD": "1000"},
    ):
        yield


@pytest.fixture(name="always_patch_app_get_storage", scope="module", autouse=True)
def always_patch_app_get_storage_fixture(storage):
    """
    Patch app.get_storage for all tests in this module
    """
    with patch("featurebyte.app.get_storage", return_value=storage):
        yield


@pytest.fixture(name="deployment_name", scope="module")
def deployment_name_fixture():
    """
    Fixture for deployment name
    """
    return "External feature list deployment"


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
        "redis": redis.from_url(TEST_REDIS_URI),
        "redis_uri": TEST_REDIS_URI,
    }
    return LazyAppContainer(app_container_config=app_container_config, instance_map=instance_map)


@pytest.fixture(name="udf_cos", scope="module")
def udf_cos_fixture(catalog):
    """
    Fixture for cosine similarity UDF
    """
    _ = catalog
    udf = fb.UserDefinedFunction.create(
        name="udf_cos",
        sql_function_name="cos",
        function_parameters=[
            fb.FunctionParameter(name="x", dtype=DBVarType.FLOAT),
        ],
        output_dtype=DBVarType.FLOAT,
        is_global=False,
    )
    yield udf


@pytest.fixture(name="features", scope="module")
def features_fixture(event_table, scd_table, source_type, item_table, udf_cos):
    """
    Fixture for feature
    """
    _ = udf_cos

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
    feature_2 = fb.UDF.udf_cos(feature_2)
    feature_2.name = feature_1.name + "_TIMES_100_COS"

    # Feature with a different entity
    feature_3 = event_view.groupby("PRODUCT_ACTION").aggregate_over(
        None,
        method="count",
        windows=["7d"],
        feature_names=["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"],
    )["EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d"]

    # Complex feature with multiple unrelated entities and request column
    feature_4 = feature_1 + feature_3 + fb.RequestColumn.point_in_time().dt.day.sin()
    feature_4.name = "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE"

    # Feature without entity
    feature_5 = event_view.groupby([]).aggregate_over(
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
    )["amount_sum_across_action_24d"]

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
        None, method="count", feature_name="Current Number of Users With This Status"
    )
    feature_11.name = "Current Number of Users With This Status"

    item_view = item_table.get_view()
    item_type_counts = item_view.groupby("order_id", category="item_type").aggregate(
        None, method="count", feature_name="my_item_feature"
    )
    feature_12 = item_type_counts.cd.most_frequent()
    feature_12.name = "Most Frequent Item Type by Order"

    # feature_10 is a lookup feature by "User". feature_11 is as-at feature by "User Status". "User
    # Status" is a parent entity of "User", so the primary entity of feature_13 is "User".
    feature_13 = feature_10 + "_" + feature_11.astype(str)
    assert feature_13.primary_entity_ids == feature_10.primary_entity_ids
    feature_13.name = "Complex Feature by User"

    feature_14 = event_view.groupby(
        ["ÜSER ID", "PRODUCT_ACTION"],
    ).aggregate_over(
        "ÀMOUNT",
        method="sum",
        windows=["24d"],
        feature_names=["Amount Sum by Customer x Product Action 24d"],
    )["Amount Sum by Customer x Product Action 24d"]

    # Add hour feature
    event_view["non_string_key"] = event_view["ËVENT_TIMESTAMP"].dt.day_of_week
    scd_view["non_string_key"] = scd_view["Effective Timestamp"].dt.day_of_week
    dict_feature = event_view.groupby("ÜSER ID", category="non_string_key").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["dict_feature"],
    )["dict_feature"]
    key_feature = scd_view["non_string_key"].as_feature("non_string_key_feature")
    feature_15 = dict_feature.cd.get_relative_frequency(key_feature)
    feature_15.name = "Relative Frequency 7d"

    # Add get value feature
    feature_16 = dict_feature.cd.get_value(key="1")
    feature_16.name = "Get Value 7d"

    feature_17 = (
        event_view[event_view["ÀMOUNT"].notnull()]
        .groupby("ÜSER ID")
        .aggregate_over(
            "ÀMOUNT",
            method="latest",
            windows=[None],
            feature_names=["Latest Amount by User"],
        )["Latest Amount by User"]
    )

    feature_18 = (
        event_view[event_view["ÀMOUNT"].notnull()]
        .groupby("ÜSER ID")
        .aggregate_over(
            "ÀMOUNT",
            method="latest",
            windows=[None],
            offset="1d",
            feature_names=["Latest Amount by User Offset 1d"],
        )["Latest Amount by User Offset 1d"]
    )
    feature_19 = event_view.groupby("ÜSER ID").aggregate_over(
        "PRODUCT_ACTION",
        method="count_distinct",
        windows=["48h"],
        feature_names=["Number of Distinct Product Action 48h"],
    )["Number of Distinct Product Action 48h"]

    # Save all features to be deployed
    features = [
        feature
        for feature in [
            feature_1,  # User entity
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
            feature_12,  # Order entity
            feature_13,
            feature_14,  # User x ProductAction composite entities
            feature_15,
            feature_16,
            feature_17,
            feature_18,
            feature_19,
        ]
        if feature is not None
    ]
    for feature in features:
        feature.save(conflict_resolution="retrieve")
        feature.update_readiness("PRODUCTION_READY")

    return features


@pytest_asyncio.fixture(name="saved_first_ten_feature_list", scope="module")
def saved_first_ten_feature_list_fixture(features):
    """
    Fixture for a saved feature list
    """
    feature_list = fb.FeatureList(features[:10], name="EXTERNAL_FS_FIRST_TEN_FEATURE_LIST")
    feature_list.save()
    yield feature_list


@pytest_asyncio.fixture(name="saved_last_ten_feature_list", scope="module")
def saved_last_ten_feature_list_fixture(features):
    """
    Fixture for a saved feature list
    """
    feature_list = fb.FeatureList(features[:10], name="EXTERNAL_FS_LAST_TEN_FEATURE_LIST")
    feature_list.save()
    yield feature_list


@pytest_asyncio.fixture(name="saved_feature_list_all", scope="module")
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
    saved_feature_list_all,
    saved_feature_list_composite_entities,
    saved_feature_list_item_type_feature,
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
    _ = saved_feature_list_all
    _ = saved_feature_list_composite_entities
    _ = saved_feature_list_item_type_feature

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


async def _deploy_feature_list(app_container, saved_feature_list, deployment_name):
    deploy_service = app_container.deploy_service
    with patch(
        "featurebyte.service.feature_manager.datetime", autospec=True
    ) as mock_feature_manager_datetime:
        mock_feature_manager_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        mock_feature_manager_datetime.side_effect = datetime
        deployment = saved_feature_list.deploy(deployment_name)
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
            assert feature_list_model.feast_enabled
            return deployment


@pytest_asyncio.fixture(name="deployed_feature_list", scope="module")
async def deployed_feature_list_fixture(
    session,
    saved_first_ten_feature_list,
    saved_last_ten_feature_list,
    saved_feature_list_all,
    removed_relationships,
    app_container,
    deployment_name,
):
    """
    Fixture for deployed feature list
    """
    _ = removed_relationships

    deployments = []
    for saved_fl in [
        saved_first_ten_feature_list,
        saved_last_ten_feature_list,
        saved_feature_list_all,
    ]:
        print(f"Deploying feature list {saved_fl.name}")
        deployment = await _deploy_feature_list(
            app_container=app_container,
            saved_feature_list=saved_fl,
            deployment_name=(
                deployment_name if saved_fl.name == saved_feature_list_all.name else None
            ),
        )
        deployments.append(deployment)

    yield deployments[-1]

    for deployment in deployments:
        await app_container.deploy_service.update_deployment(
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


@pytest.fixture(name="order_use_case", scope="module")
def order_use_case_fixture(order_entity):
    """
    Fixture for order level use case
    """
    target = fb.TargetNamespace.create(
        "order_target",
        primary_entity=[order_entity.name],
        dtype=DBVarType.FLOAT,
    )
    context = fb.Context.create(
        name="order_context",
        primary_entity=[order_entity.name],
    )
    use_case = fb.UseCase.create("order_use_case", target.name, context.name, "order_description")
    return use_case


@pytest_asyncio.fixture(name="deployed_feature_list_composite_entities", scope="module")
async def deployed_features_list_composite_entities_fixture(
    saved_feature_list_composite_entities, app_container
):
    """
    Fixture for deployed feature list
    """
    deployment = await _deploy_feature_list(
        app_container=app_container,
        saved_feature_list=saved_feature_list_composite_entities,
        deployment_name="External feature list deployment composite entities",
    )

    yield deployment

    await app_container.deploy_service.update_deployment(
        deployment_id=deployment.id,
        to_enable_deployment=False,
    )


@pytest_asyncio.fixture(
    name="deployed_feature_list_composite_entities_order_use_case", scope="module"
)
async def deployed_features_list_composite_entities_order_use_case_fixture(
    saved_feature_list_composite_entities,
    deployed_feature_list_composite_entities,
    app_container,
    order_use_case,
):
    """
    Fixture for deployed feature list.

    The same feature list was deployed (via deployed_feature_list_composite_entities) but not with
    the order use case. Creating this deployment now requires lookup feature tables to be created.
    """
    _ = deployed_feature_list_composite_entities

    feature_list = saved_feature_list_composite_entities

    deploy_service = app_container.deploy_service
    with patch(
        "featurebyte.service.feature_manager.datetime",
        autospec=True,
    ) as mock_feature_manager_datetime:
        mock_feature_manager_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        deployment = feature_list.deploy(
            "deployment order use case", use_case_name=order_use_case.name
        )
        with patch("featurebyte.service.feature_materialize.datetime") as mock_datetime:
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


@pytest.fixture(name="item_use_case", scope="module")
def item_use_case_fixture(item_entity):
    """
    Fixture for item level use case
    """
    target = fb.TargetNamespace.create(
        "item_target",
        primary_entity=[item_entity.name],
        dtype=DBVarType.FLOAT,
    )
    context = fb.Context.create(
        name="item_context",
        primary_entity=[item_entity.name],
    )
    use_case = fb.UseCase.create("item_use_case", target.name, context.name, "item_description")
    return use_case


@pytest_asyncio.fixture(name="saved_feature_list_item_type_feature", scope="module")
async def saved_feature_list_item_type_feature_fixture(item_table):
    """
    Fixture for a saved feature list with item type feature
    """
    item_view = item_table.get_view()
    feature = item_view.groupby("item_type").aggregate_over(
        value_column=None,
        method="count",
        windows=["7d"],
        feature_names=["Count 7d by Item Type"],
    )["Count 7d by Item Type"]
    feature.save()
    feature.update_readiness("PRODUCTION_READY")

    feature_list = fb.FeatureList([feature], name=f"{feature.name} Feature List")
    feature_list.save()
    return feature_list


@pytest_asyncio.fixture(name="deployed_feature_list_item_use_case", scope="module")
async def deployed_feature_list_item_use_case_fixture(
    app_container,
    saved_feature_list_item_type_feature,
    item_use_case,
):
    """
    Fixture for a deployed feature list with a ItemType feature to be served by Item entity (child
    of ItemType)
    """
    deploy_service = app_container.deploy_service
    with patch(
        "featurebyte.service.feature_manager.datetime", autospec=True
    ) as mock_feature_manager_datetime:
        mock_feature_manager_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        deployment = saved_feature_list_item_type_feature.deploy(
            "deployment item use case", use_case_name=item_use_case.name
        )
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
        query_filter={"precomputed_lookup_feature_table_info": None},
    ):
        primary_entity_to_feature_table[feature_table.name] = feature_table
    return primary_entity_to_feature_table


@pytest_asyncio.fixture(name="offline_store_feature_tables_all", scope="module")
async def offline_store_feature_tables_all_fixture(app_container, deployed_feature_list):
    """
    Fixture for offline store feature tables based on the deployed features, including precomputed
    lookup feature tables
    """
    _ = deployed_feature_list
    primary_entity_to_feature_table = {}
    async for (
        feature_table
    ) in app_container.offline_store_feature_table_service.list_documents_iterator(query_filter={}):
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


def get_relationship_info(feature_list_model, child_entity, parent_entity):
    """
    Helper function to get a relationship info between two entities
    """
    for info in feature_list_model.relationships_info:
        if info.entity_id == child_entity.id and info.related_entity_id == parent_entity.id:
            return info
    raise AssertionError(
        f"Relationship with child as {child_entity.name} and parent as {parent_entity.name} not found"
    )


@pytest.fixture(name="expected_feature_table_names")
def expected_feature_table_names_fixture():
    """
    Fixture for expected feature table names
    """
    expected = {
        "cat1__no_entity_1h",
        "cat1_product_action_1h",
        "cat1_cust_id_1h",
        "cat1_userid_1h",
        "cat1_userid_1h_1",
        "cat1_userid_1d",
        "cat1_user_status_1d",
        "cat1_order_id_1d",
        "cat1_userid_product_action_1h",
    }
    return expected


@pytest.mark.order(1)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
def test_feature_tables_expected(
    offline_store_feature_tables,
    expected_feature_table_names,
):
    """
    Test offline store feature tables are created as expected
    """
    assert set(offline_store_feature_tables.keys()) == expected_feature_table_names


@pytest.mark.order(1)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
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
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
@pytest.mark.asyncio
async def test_feature_tables_populated(session, offline_store_feature_tables_all, source_type):
    """
    Check feature tables are populated correctly
    """
    for feature_table in offline_store_feature_tables_all.values():
        df = await session.execute_query(
            sql_to_string(
                parse_one(f'SELECT * FROM "{feature_table.name}"'),
                session.source_type,
            )
        )

        # Should not be empty
        assert df.shape[0] > 0

        # Should have all the serving names and output columns tracked in OfflineStoreFeatureTable
        expected = set([InternalName.FEATURE_TIMESTAMP_COLUMN.value] + feature_table.serving_names)
        if feature_table.precomputed_lookup_feature_table_info is None:
            output_column_names = feature_table.output_column_names
        else:
            output_column_names = next(
                table
                for table in offline_store_feature_tables_all.values()
                if table.id
                == feature_table.precomputed_lookup_feature_table_info.source_feature_table_id
            ).output_column_names
        expected.update(output_column_names)
        if len(feature_table.serving_names) > 0:
            expected.add(" x ".join(feature_table.serving_names))
        elif source_type == SourceType.DATABRICKS_UNITY:
            expected.add(DUMMY_ENTITY_COLUMN_NAME)
        assert set(df.columns.tolist()) == expected

        if len(feature_table.serving_names) == 0 and source_type == SourceType.DATABRICKS_UNITY:
            assert (df[DUMMY_ENTITY_COLUMN_NAME] == DUMMY_ENTITY_VALUE).all()


async def _check_udf_with_container_input(
    session, udfs_for_on_demand_func, offline_store_feature_tables
):
    """Test that udf is created in databricks"""
    # check UDF taking count dictionary input as input
    table_name = "cat1_cust_id_1h"
    cos_sim_udf = next(
        udf
        for udf in udfs_for_on_demand_func
        if udf.startswith("udf_external_fs_cosine_similarity_")
    )
    schema_name = session.schema_name
    table = offline_store_feature_tables[table_name]
    col_name = next(
        colname
        for colname in table.output_column_names
        if colname.startswith("__EXTERNAL_FS_COSINE_SIMILARITY_")
    )
    query = f"""
    SELECT {cos_sim_udf}({col_name}, {col_name}) AS cos_sim
    FROM {schema_name}.{table_name}
    """
    df = await session.execute_query(query)
    assert df.shape[0] > 0
    assert sorted(df.cos_sim.unique()) == [0.0, 1.0]


@pytest.mark.order(3)
@pytest.mark.parametrize("source_type", ["databricks_unity"], indirect=True)
@pytest.mark.asyncio
async def test_databricks_udf_created(session, offline_store_feature_tables, source_type):
    """Test that udf is created in databricks"""
    df = await session.execute_query(
        sql_to_string(parse_one("SHOW USER FUNCTIONS"), session.source_type)
    )
    all_udfs = set(df.function.apply(lambda x: x.split(".")[-1]).to_list())
    assert len(all_udfs) > 0
    udfs_for_on_demand_func = [udf for udf in all_udfs if udf.startswith("udf_")]
    if source_type == SourceType.DATABRICKS_UNITY:
        # udf_external_fs_complex_user_x_production_action_feature_<version>_<feature_id>
        # udf_external_fs_cosine_similarity_<version>_<feature_id>
        # udf_relativefrequency7d_<version>_<feature_id>
        assert len(udfs_for_on_demand_func) == 3

        await _check_udf_with_container_input(
            session, udfs_for_on_demand_func, offline_store_feature_tables
        )
    else:
        assert len(udfs_for_on_demand_func) == 0


@pytest.mark.order(4)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
@pytest.mark.asyncio
@pytest.mark.usefixtures("deployed_feature_list", "deployed_feature_list_composite_entities")
async def test_feast_registry(
    app_container,
    offline_store_feature_tables_all,
    source_type,
    deployment_name,
):
    """
    Check feast registry is populated correctly
    """
    deployment = None
    async for deployment in app_container.deployment_service.list_documents_iterator(
        query_filter={"name": deployment_name}
    ):
        break

    assert deployment is not None
    feature_store = (
        await app_container.feast_feature_store_service.get_feast_feature_store_for_deployment(
            deployment=deployment
        )
    )

    # check feature views (DO NOT CALL list_feature_views() as it will cache the list of feature views
    # and has side effect on the following call, this should be Feast specific issue).
    assert {
        fv.name
        for fv in feature_store._list_feature_views(allow_cache=False, hide_dummy_entity=False)
    } == {table.name for table in offline_store_feature_tables_all.values()}

    # Check feature services
    feature_service_name = f"EXTERNAL_FS_FEATURE_LIST_{get_version()}"
    assert {fs.name for fs in feature_store.list_feature_services()} == {feature_service_name}

    # Check feast materialize and get_online_features
    feature_store = (
        await app_container.feast_feature_store_service.get_feast_feature_store_for_deployment(
            deployment=deployment
        )
    )
    feature_service = feature_store.get_feature_service(feature_service_name)
    version = get_version()
    entity_row = {
        "order_id": "T3850",
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 12:00:00"),
    }
    with patch(
        "feast.utils._augment_response_with_on_demand_transforms"
    ) as mock_augment_response_with_on_demand_transforms:
        mock_augment_response_with_on_demand_transforms.side_effect = (
            augment_response_with_on_demand_transforms
        )
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
        f"Amount Sum by Customer x Product Action 24d_{version}": [169.77000427246094],
        f"Current Number of Users With This Status_{version}": [1],
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}": [
            {
                "__MISSING__": 174.39,
                "detail": 169.77,
                "purchase": 473.31,
                "rëmove": 102.37,
                "àdd": 27.1,
            }
        ],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}": [612.9199829101562],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_COS_{version}": [np.cos(61292)],
        f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}": [
            [
                0.5147396926226291,
                0.5105985934506334,
                0.4033489230325782,
                0.5249651077128239,
                0.5478807119010883,
                0.4721197245416274,
                0.4021979871512357,
                0.4755591916849737,
                0.4894565525521825,
                0.4121823297275281,
            ]
        ],
        f"EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}": [656.8292846679688],
        f"EXTERNAL_FS_COSINE_SIMILARITY_{version}": [0.0],
        f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}": [0.9210227727890015],
        f"EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d_{version}": [43],
        f"EXTERNAL_FS_COUNT_OVERALL_7d_{version}": [200],
        f"Most Frequent Item Type by Order_{version}": ["type_24"],
        f"User Status Feature_{version}": ["STÀTUS_CODE_26"],
        f"Complex Feature by User_{version}": ["STÀTUS_CODE_26_1"],
        f"Relative Frequency 7d_{version}": [0.6086956262588501],
        f"Latest Amount by User_{version}": [91.31999969482422],
        f"Latest Amount by User Offset 1d_{version}": [10.229999542236328],
        f"Number of Distinct Product Action 48h_{version}": [5],
        f"Get Value 7d_{version}": [8],
        "order_id": ["T3850"],
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop(f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}")
        expected.pop(f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}")
    assert_dict_approx_equal(online_features, expected)

    # set point in time > 2001-01-02 12:00:00 +  2 hours (frequency is 1 hour) &
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 14:00:01")
    with patch(
        "feast.utils._augment_response_with_on_demand_transforms"
    ) as mock_augment_response_with_on_demand_transforms:
        mock_augment_response_with_on_demand_transforms.side_effect = (
            augment_response_with_on_demand_transforms
        )
        online_features = feature_store.get_online_features(
            features=feature_service,
            entity_rows=[entity_row],
        ).to_dict()
    expected = {
        "order_id": ["T3850"],
        f"Amount Sum by Customer x Product Action 24d_{version}": [None],
        f"User Status Feature_{version}": ["STÀTUS_CODE_26"],
        f"Current Number of Users With This Status_{version}": [1],
        f"Complex Feature by User_{version}": ["STÀTUS_CODE_26_1"],
        f"Relative Frequency 7d_{version}": [None],
        f"Most Frequent Item Type by Order_{version}": ["type_24"],
        f"EXTERNAL_FS_COUNT_OVERALL_7d_{version}": [None],
        f"EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}": [None],
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_COS_{version}": [None],
        f"EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}": [None],
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}": [None],
        f"EXTERNAL_FS_COSINE_SIMILARITY_{version}": [None],
        f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}": [None],
        # due to implementation in _get_vector_cosine_similarity_function_name,
        # any null value in the input vector will result in 0 cosine similarity
        f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}": [0.0],
        f"Latest Amount by User_{version}": [91.31999969482422],
        f"Latest Amount by User Offset 1d_{version}": [10.229999542236328],
        f"Number of Distinct Product Action 48h_{version}": [None],
        f"Get Value 7d_{version}": [None],
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop(f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}")
        expected.pop(f"EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}")
    assert_dict_approx_equal(online_features, expected)

    # set the point in time earlier than the 2001-01-02 12:00:00
    # expect all ttl features to be null
    entity_row["POINT_IN_TIME"] = pd.Timestamp("2001-01-02 11:59:59")
    with patch(
        "feast.utils._augment_response_with_on_demand_transforms"
    ) as mock_augment_response_with_on_demand_transforms:
        mock_augment_response_with_on_demand_transforms.side_effect = (
            augment_response_with_on_demand_transforms
        )
        online_features = feature_store.get_online_features(
            features=feature_service,
            entity_rows=[entity_row],
        ).to_dict()
    assert_dict_approx_equal(online_features, expected)


@pytest.mark.order(5)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
def test_online_features__all_entities_provided(config, deployed_feature_list, source_type):
    """
    Check online features are populated correctly
    """
    client = config.get_client()
    deployment = deployed_feature_list

    # Note that the serving key for the deployment is order_id. The additional entities provided
    # will not be used.
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
        "Amount Sum by Customer x Product Action 24d": None,
        "Complex Feature by User": None,
        "Current Number of Users With This Status": None,
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": {
            "__MISSING__": 234.77,
            "detail": 235.24,
            "purchase": 302.64,
            "rëmove": 11.39,
            "àdd": 338.51,
        },
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": 552.239990234375,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_COS": 0.4675004780292511,
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            0.4131900194993384,
            0.3647896782902609,
            0.5372172175764314,
            0.4364497061486116,
            0.5284909604747903,
            0.4423862862877522,
            0.3146524937754731,
            0.3935903526211379,
            0.6550828931554313,
            0.3655393355048817,
        ],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": None,
        "EXTERNAL_FS_COSINE_SIMILARITY": 0.0,
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": 0.8980490565299988,
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": None,
        "EXTERNAL_FS_COUNT_OVERALL_7d": 200,
        "Most Frequent Item Type by Order": "type_12",
        "Relative Frequency 7d": None,
        "PRODUCT_ACTION": "detail",
        "User Status Feature": None,
        "Latest Amount by User": 76.86000061035156,
        "Latest Amount by User Offset 1d": 78.93000030517578,
        "cust_id": 761,
        "order_id": "T1230",
        "user_status": "STÀTUS_CODE_37",
        "üser id": 5,
        "Number of Distinct Product Action 48h": 5,
        "Get Value 7d": 5,
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
        "Amount Sum by Customer x Product Action 24d": 169.77000427246094,
        "Latest Amount by User": 91.31999969482422,
        "Latest Amount by User Offset 1d": 10.229999542236328,
        "Complex Feature by User": "STÀTUS_CODE_26_1",
        "Current Number of Users With This Status": 1,
        "EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d": {
            "__MISSING__": 174.39,
            "detail": 169.77,
            "purchase": 473.31,
            "rëmove": 102.37,
            "àdd": 27.1,
        },
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h": 612.9199829101562,
        "EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_COS": 0.8903552293777466,
        "EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h": [
            0.5147396926226291,
            0.5105985934506334,
            0.4033489230325783,
            0.5249651077128239,
            0.5478807119010886,
            0.4721197245416274,
            0.4021979871512356,
            0.4755591916849737,
            0.4894565525521825,
            0.4121823297275281,
        ],
        "EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE": 656.8292846679688,
        "EXTERNAL_FS_COSINE_SIMILARITY": 0.0,
        "EXTERNAL_FS_COSINE_SIMILARITY_VEC": 0.9210227727890015,
        "EXTERNAL_FS_COUNT_BY_PRODUCT_ACTION_7d": 43,
        "EXTERNAL_FS_COUNT_OVERALL_7d": 200,
        "Most Frequent Item Type by Order": "type_24",
        "User Status Feature": "STÀTUS_CODE_26",
        "order_id": "T3850",
        "Relative Frequency 7d": 0.6086956262588501,
        "Number of Distinct Product Action 48h": 5,
        "Get Value 7d": 8,
    }
    if source_type == SourceType.DATABRICKS_UNITY:
        expected.pop("EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h")
        expected.pop("EXTERNAL_FS_COSINE_SIMILARITY_VEC")
    return expected


def process_output_features_helper(feat_dict, source_type):
    """
    Process output features to facilitate testing
    """

    def json_loads_if_str(val):
        if isinstance(val, str):
            return json.loads(val)
        return val

    feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"] = json_loads_if_str(
        feat_dict["EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d"]
    )
    if source_type != SourceType.DATABRICKS_UNITY:
        feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"] = json_loads_if_str(
            feat_dict["EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h"]
        )


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
@pytest.mark.asyncio
async def test_batch_features(
    session, data_source, deployed_feature_list, expected_features_order_id_T3850, source_type
):
    """
    Test batch features computation
    """
    df_request = pd.DataFrame({"order_id": ["T3850"]})
    table_name = f"batch_request_table_{ObjectId()}".upper()
    await session.register_table(table_name, df_request)
    batch_request_table = data_source.get_source_table(
        table_name=table_name,
        schema_name=session.schema_name,
        database_name=session.database_name,
    )
    with patch(
        "featurebyte.query_graph.sql.online_serving.datetime", autospec=True
    ) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        batch_feature_table = deployed_feature_list.compute_batch_feature_table(
            batch_request_table, table_name
        )
    df_batch_features = batch_feature_table.to_pandas()
    feat_dict = df_batch_features.iloc[0].to_dict()
    process_output_features_helper(feat_dict, source_type)
    assert_dict_approx_equal(feat_dict, expected_features_order_id_T3850)


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
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
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
def test_online_features__invalid_child_entity(config, deployed_feature_list):
    """
    Check online features using a child entity that is a feature list's supported_serving_entity_ids
    but not the deployment's expected serving entity
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
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
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
    expected_non_existing_order_id_features.update({
        feature_name: None
        for feature_name in expected_features_order_id_T3850
        if feature_name != "order_id"
    })
    expected_non_existing_order_id_features["EXTERNAL_FS_COUNT_OVERALL_7d"] = 200
    if source_type != SourceType.DATABRICKS_UNITY:
        expected_non_existing_order_id_features["EXTERNAL_FS_COSINE_SIMILARITY_VEC"] = 0
    assert_dict_approx_equal(features[0], expected_features_order_id_T3850)
    assert_dict_approx_equal(features[1], expected_non_existing_order_id_features)


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
def test_online_features__composite_entities(config, deployed_feature_list_composite_entities):
    """
    Check online features when the feature list only has a feature with composite entities
    """
    client = config.get_client()
    deployment = deployed_feature_list_composite_entities

    entity_serving_names = [
        {
            "PRODUCT_ACTION": "detail",
            "üser id": 7,
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
        "PRODUCT_ACTION": "detail",
        "üser id": 7,
    }
    assert_dict_approx_equal(feat_dict, expected)


@pytest.mark.order(6)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
def test_online_features__composite_entities_via_order_id(
    config, deployed_feature_list_composite_entities_order_use_case
):
    """
    Check online features when the feature list only has a feature with composite entities
    """
    client = config.get_client()
    deployment = deployed_feature_list_composite_entities_order_use_case

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
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
@pytest.mark.asyncio
async def test_simulated_materialize__ttl_feature_table(
    app_container,
    session,
    user_entity_ttl_feature_table,
    source_type,
    config,
):
    """
    Test simulating scheduled feature materialization for a feature table with TTL
    """
    # Check calling scheduled_materialize_features()
    service = app_container.feature_materialize_service
    feature_table_model = user_entity_ttl_feature_table
    with patch("featurebyte.service.feature_materialize.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 13)
        await service.scheduled_materialize_features(feature_table_model=feature_table_model)
    df = await session.execute_query(
        sql_to_string(
            parse_one(f'SELECT * FROM "{feature_table_model.name}"'), session.source_type
        ),
    )
    version = get_version()
    expected = [
        "__feature_timestamp",
        "üser id",
        f"EXTERNAL_CATEGORY_AMOUNT_SUM_BY_USER_ID_7d_{version}",
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_TIMES_100_COS_{version}",
        f"EXTERNAL_FS_AMOUNT_SUM_BY_USER_ID_24h_{version}",
        f"Number of Distinct Product Action 48h_{version}",
        f"__EXTERNAL_FS_COSINE_SIMILARITY_{version}__part0",
        f"__EXTERNAL_FS_COMPLEX_USER_X_PRODUCTION_ACTION_FEATURE_{version}__part0",
        f"__Relative Frequency 7d_{version}__part0",
    ]
    if source_type != SourceType.DATABRICKS_UNITY:
        expected += [
            f"EXTERNAL_FS_ARRAY_AVG_BY_USER_ID_24h_{version}",
            f"__EXTERNAL_FS_COSINE_SIMILARITY_VEC_{version}__part0",
        ]

    assert set(df.columns.tolist()) == set(expected)
    assert df.shape[0] == 18
    assert df["__feature_timestamp"].nunique() == 2
    assert df["üser id"].isnull().sum() == 0

    # Materialize one more time
    with patch("featurebyte.service.feature_materialize.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 14)
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
    with patch("featurebyte.service.feature_materialize.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 15)
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

    client = config.get_client()
    response = client.get(
        "/system_metrics",
        params={
            "offline_store_feature_table_id": str(feature_table_model.id),
            "metrics_type": "scheduled_feature_materialize",
        },
    )
    assert response.status_code == 200
    response_dict = response.json()
    pprint(response_dict["data"])
    assert len(response_dict["data"]) == 3


async def reload_feature_table_model(app_container, feature_table_model):
    """
    Reload feature table model from persistent
    """
    feature_table_service = app_container.offline_store_feature_table_service
    feature_table_model = await feature_table_service.get_document(feature_table_model.id)
    return feature_table_model


@pytest.mark.order(8)
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
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
    assert feature_table_model.aggregation_ids == []

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
        f"__Relative Frequency 7d_{version}__part1",
    ]
    assert df_0.shape[0] == 9
    assert df_0["__feature_timestamp"].nunique() == 1

    # Trigger a materialization task after the feature table is created. This should materialize
    # features for the entities that appear since deployment time (mocked above) till now.
    task_payload = FeatureMaterializeSyncTaskPayload(
        catalog_id=feature_table_model.catalog_id,
        offline_store_feature_table_name=feature_table_model.name,
        offline_store_feature_table_id=feature_table_model.id,
    )
    await app_container.task_manager.submit(task_payload)
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
    await app_container.task_manager.submit(task_payload)
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
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY, indirect=True)
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


@pytest.mark.order(11)
def test_item_view_feature_online_serving(config, deployed_feature_list_item_use_case):
    """
    Test item view feature with parent serving
    """
    client = config.get_client()
    deployment = deployed_feature_list_item_use_case

    entity_serving_names = [
        {
            "item_id": "item_60",
        }
    ]
    data = OnlineFeaturesRequestPayload(entity_serving_names=entity_serving_names)

    with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(2001, 1, 2, 12)
        res = client.post(
            f"/deployment/{deployment.id}/online_features",
            json=data.json_dict(),
        )
    assert res.status_code == 200

    feat_dict = res.json()["features"][0]
    expected = {"item_id": "item_60", "Count 7d by Item Type": 11}
    assert_dict_approx_equal(feat_dict, expected)
