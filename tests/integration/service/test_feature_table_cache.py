"""
Integration test for feature table cache
"""
import time

import pandas as pd
import pytest
from sqlglot import parse_one

from featurebyte import FeatureList
from featurebyte.enum import InternalName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import sql_to_string
from tests.util.helper import create_observation_table_from_dataframe


@pytest.fixture(name="feature_list")
def feature_list_fixture(feature_group, feature_group_per_category):
    """Feature List fixture"""
    feature_group["COUNT_2h / COUNT_24h"] = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    feature_list = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_24h"],
            feature_group["COUNT_2h / COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group_per_category["ENTROPY_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
            feature_group_per_category["NUM_UNIQUE_ACTION_24h"],
            feature_group_per_category["ACTION_SIMILARITY_2h_to_24h"],
        ],
        name="My Feature List for Materialization",
    )
    feature_list.save(conflict_resolution="retrieve")
    yield feature_list
    feature_list.delete()


@pytest.fixture(name="two_feature_lists")
def two_feature_lists_fixure(feature_group, feature_group_per_category):
    """Two Feature Lists fixture"""
    feature_group["COUNT_2h / COUNT_24h"] = feature_group["COUNT_2h"] / feature_group["COUNT_24h"]
    feature_list_1 = FeatureList(
        [
            feature_group["COUNT_2h"],
            feature_group["COUNT_2h / COUNT_24h"],
            feature_group_per_category["ENTROPY_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
            feature_group_per_category["NUM_UNIQUE_ACTION_24h"],
        ],
        name="My Feature List 1 for Materialization",
    )
    feature_list_1.save(conflict_resolution="retrieve")

    feature_list_2 = FeatureList(
        [
            feature_group["COUNT_2h / COUNT_24h"],
            feature_group["COUNT_24h"],
            feature_group_per_category["COUNT_BY_ACTION_24h"],
            feature_group_per_category["MOST_FREQUENT_ACTION_24h"],
            feature_group_per_category["ACTION_SIMILARITY_2h_to_24h"],
        ],
        name="My Feature List 2 for Materialization",
    )
    feature_list_2.save(conflict_resolution="retrieve")

    return feature_list_1, feature_list_2


@pytest.fixture(name="observation_table")
def observation_table_fixture(event_view):
    """Observation table fixture"""
    return event_view.create_observation_table(
        name=f"observation_table_{time.time()}",
        sample_rows=50,
        columns=["ËVENT_TIMESTAMP", "ÜSER ID"],
        columns_rename_mapping={
            "ËVENT_TIMESTAMP": "POINT_IN_TIME",
            "ÜSER ID": "üser id",
        },
        primary_entities=["User"],
    )


@pytest.mark.asyncio
async def test_create_feature_table_cache(
    feature_store,
    session,
    data_source,
    feature_list,
    observation_table,
    feature_store_service,
    observation_table_service,
    feature_list_service,
    feature_service,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    source_type,
):
    """Test create feature table cache"""
    feature_store_model = await feature_store_service.get_document(document_id=feature_store.id)
    observation_table_model = await observation_table_service.get_document(
        document_id=observation_table.id
    )
    feature_list_model = await feature_list_service.get_document(document_id=feature_list.id)

    feature_cluster = feature_list_model.feature_clusters[0]
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store_model,
        observation_table=observation_table_model,
        graph=feature_cluster.graph,
        nodes=feature_cluster.nodes,
        feature_list_id=feature_list_model.id,
    )

    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    features = [definition.feature_name for definition in feature_table_cache.feature_definitions]
    hashes = [definition.definition_hash for definition in feature_table_cache.feature_definitions]
    assert len(features) == len(feature_list_model.feature_ids)

    feature_hashes = []
    async for document in feature_service.list_documents_iterator(
        query_filter={"_id": {"$in": feature_list_model.feature_ids}}
    ):
        feature_hashes.append(document.definition_hash)

    assert set(hashes) == set(feature_hashes)

    query = sql_to_string(
        parse_one(
            f"""
            SELECT * FROM "{session.database_name}"."{session.schema_name}"."{feature_table_cache.table_name}"
            """
        ),
        source_type=source_type,
    )
    df = await session.execute_query(query)
    assert df.shape[0] == 50
    observation_table_cols = list({col.name for col in observation_table_model.columns_info})
    assert set(df.columns.tolist()) == set(
        [InternalName.TABLE_ROW_INDEX] + observation_table_cols + features
    )


@pytest.mark.asyncio
async def test_update_feature_table_cache(
    feature_store,
    session,
    data_source,
    two_feature_lists,
    observation_table,
    feature_store_service,
    observation_table_service,
    feature_list_service,
    feature_service,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    source_type,
):
    """Test update feature table cache"""
    feature_store_model = await feature_store_service.get_document(document_id=feature_store.id)
    observation_table_model = await observation_table_service.get_document(
        document_id=observation_table.id
    )
    feature_list_model_1 = await feature_list_service.get_document(
        document_id=two_feature_lists[0].id
    )
    feature_list_model_2 = await feature_list_service.get_document(
        document_id=two_feature_lists[1].id
    )

    feature_cluster = feature_list_model_1.feature_clusters[0]
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store_model,
        observation_table=observation_table_model,
        graph=feature_cluster.graph,
        nodes=feature_cluster.nodes,
        feature_list_id=feature_list_model_1.id,
    )
    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    features_1_fl = [
        definition.feature_name for definition in feature_table_cache.feature_definitions
    ]
    assert len(features_1_fl) == len(feature_list_model_1.feature_ids)

    feature_cluster = feature_list_model_2.feature_clusters[0]
    await feature_table_cache_service.create_or_update_feature_table_cache(
        feature_store=feature_store_model,
        observation_table=observation_table_model,
        graph=feature_cluster.graph,
        nodes=feature_cluster.nodes,
        feature_list_id=feature_list_model_2.id,
    )
    feature_table_cache = (
        await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
            observation_table_id=observation_table.id,
        )
    )
    features = [definition.feature_name for definition in feature_table_cache.feature_definitions]
    hashes = [definition.definition_hash for definition in feature_table_cache.feature_definitions]

    assert set(features_1_fl) <= set(features)

    combined_feature_ids = set(feature_list_model_1.feature_ids + feature_list_model_2.feature_ids)
    assert len(features) == len(combined_feature_ids)

    feature_hashes = []
    async for document in feature_service.list_documents_iterator(
        query_filter={"_id": {"$in": list(combined_feature_ids)}}
    ):
        feature_hashes.append(document.definition_hash)

    assert set(hashes) == set(feature_hashes)

    query = sql_to_string(
        parse_one(
            f"""
            SELECT * FROM "{session.database_name}"."{session.schema_name}"."{feature_table_cache.table_name}"
            """
        ),
        source_type=source_type,
    )
    df = await session.execute_query(query)
    assert df.shape[0] == 50
    observation_table_cols = list({col.name for col in observation_table_model.columns_info})
    assert set(df.columns.tolist()) == set(
        [InternalName.TABLE_ROW_INDEX] + observation_table_cols + features
    )


@pytest.mark.asyncio
async def test_create_view_from_cache(
    feature_store,
    session,
    data_source,
    feature_list,
    observation_table,
    feature_store_service,
    observation_table_service,
    feature_list_service,
    feature_service,
    feature_table_cache_service,
    feature_table_cache_metadata_service,
    source_type,
):
    """Test create view from feature table cache"""
    feature_store_model = await feature_store_service.get_document(document_id=feature_store.id)
    observation_table_model = await observation_table_service.get_document(
        document_id=observation_table.id
    )
    feature_list_model = await feature_list_service.get_document(document_id=feature_list.id)

    view_details = TableDetails(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="RESULT_VIEW",
    )
    feature_cluster = feature_list_model.feature_clusters[0]
    nodes = feature_cluster.nodes[:5]
    observation_table_cols = list({col.name for col in observation_table_model.columns_info})
    feature_names = [feature_cluster.graph.get_node_output_column_name(node.name) for node in nodes]
    await feature_table_cache_service.create_view_from_cache(
        feature_store=feature_store_model,
        observation_table=observation_table_model,
        graph=feature_cluster.graph,
        nodes=nodes,
        output_view_details=view_details,
        is_target=False,
        feature_list_id=feature_list_model.id,
    )

    query = sql_to_string(
        parse_one(
            f"""
            SELECT * FROM "{view_details.database_name}"."{view_details.schema_name}"."{view_details.table_name}"
            """
        ),
        source_type=source_type,
    )
    df = await session.execute_query(query)
    assert df.shape == (50, len(set(feature_names + observation_table_cols)) + 1)
    assert set(df.columns.tolist()) == set(
        [InternalName.TABLE_ROW_INDEX] + feature_names + observation_table_cols
    )

    # update cache table with second feature list
    await feature_table_cache_service.create_view_from_cache(
        feature_store=feature_store_model,
        observation_table=observation_table_model,
        graph=feature_cluster.graph,
        nodes=feature_cluster.nodes,
        output_view_details=view_details,
        is_target=False,
        feature_list_id=feature_list_model.id,
    )
    query = sql_to_string(
        parse_one(
            f"""
            SELECT * FROM "{view_details.database_name}"."{view_details.schema_name}"."{view_details.table_name}"
            """
        ),
        source_type=source_type,
    )
    df = await session.execute_query(query)
    assert len(feature_list.feature_names) == 8
    assert df.shape == (50, len(set(feature_list.feature_names + observation_table_cols)) + 1)
    assert set(df.columns.tolist()) == set(
        [InternalName.TABLE_ROW_INDEX] + feature_list.feature_names + observation_table_cols
    )
