"""
Test feature table cache service
"""

from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.feature_table_cache_metadata import (
    CachedDefinitionWithTable,
    CachedFeatureDefinition,
    FeatureTableCacheMetadataModel,
)
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.common_table import TabularSource


@pytest.fixture(name="observation_table")
def observation_table_fixture(event_table, user):
    """Observation table fixture"""
    request_input = SourceTableRequestInput(source=event_table.tabular_source)
    location = TabularSource(**{
        "feature_store_id": event_table.tabular_source.feature_store_id,
        "table_details": {
            "database_name": "fb_database",
            "schema_name": "fb_schema",
            "table_name": "fb_materialized_table",
        },
    })
    return ObservationTableModel(
        name="observation_table_from_source_table",
        location=location,
        request_input=request_input.model_dump(by_alias=True),
        columns_info=[
            {"name": "cust_id", "dtype": "INT"},
            {"name": "b", "dtype": "INT"},
            {"name": "c", "dtype": "INT"},
        ],
        num_rows=1000,
        most_recent_point_in_time="2023-01-15T10:00:00",
        user_id=user.id,
    )


@pytest.mark.asyncio
async def test_get_or_create_feature_table_cache_creates_from_scratch(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """test get_or_create_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=1,
        session=mock_snowflake_session,
    )
    assert document.id
    assert document.observation_table_id == observation_table_doc.id
    assert (
        document.table_name
        == f"{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_000000000000000000000000"
    )
    assert document.feature_definitions == []


@pytest.mark.asyncio
async def test_get_or_create_feature_table_cache_returns_existing(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """test get_or_create_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    data = FeatureTableCacheMetadataModel(
        observation_table_id=observation_table_doc.id,
        table_name="my_feature_table_cache",
        feature_definitions=[
            CachedFeatureDefinition(
                feature_id=ObjectId(),
                definition_hash="feature_hash_1",
                feature_name="feature_name_1",
            ),
            CachedFeatureDefinition(
                feature_id=ObjectId(),
                definition_hash="feature_hash_2",
                feature_name="feature_name_2",
            ),
        ],
    )
    document = await feature_table_cache_metadata_service.create_document(data)

    get_document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=1,
        session=mock_snowflake_session,
    )
    assert document == get_document


@pytest.mark.asyncio
async def test_update_feature_table_cache_from_scratch(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=1,
        session=mock_snowflake_session,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
    assert document.feature_definitions == []

    features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=document.id,
        feature_definitions=features,
    )

    document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=1,
        session=mock_snowflake_session,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
    assert document.feature_definitions == [
        CachedFeatureDefinition(
            feature_id=document.feature_definitions[0].feature_id,
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=document.feature_definitions[1].feature_id,
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
        ),
    ]


@pytest.mark.asyncio
async def test_update_feature_table_cache_add_features(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
        ),
    ]
    document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=2,
        session=mock_snowflake_session,
    )
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=document.id,
        feature_definitions=features,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id,
    )
    assert all([
        feat.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
        for feat in cached_definitions
    ])
    assert [feat.definition_hash for feat in cached_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
    ]
    assert [feat.feature_name for feat in cached_definitions] == [
        "FEATURE_feature_1_definition_hash",
        "FEATURE_feature_2_definition_hash",
    ]

    more_features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_oops_different_definition_hash",
            feature_name="FEATURE_feature_2_oops_different_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
        ),
    ]
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=document.id,
        feature_definitions=more_features,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id,
    )
    assert all([
        feat.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
        for feat in cached_definitions
    ])
    assert [feat.definition_hash for feat in cached_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
        "feature_2_oops_different_definition_hash",
        "feature_3_definition_hash",
    ]
    assert [feat.feature_name for feat in cached_definitions] == [
        "FEATURE_feature_1_definition_hash",
        "FEATURE_feature_2_definition_hash",
        "FEATURE_feature_2_oops_different_definition_hash",
        "FEATURE_feature_3_definition_hash",
    ]


@pytest.mark.asyncio
async def test_update_feature_table_cache_updates_feature_id(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    features = [
        CachedFeatureDefinition(
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
        ),
    ]
    document = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=2,
        session=mock_snowflake_session,
    )
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=document.id,
        feature_definitions=features,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id,
    )
    assert all([
        feat.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
        for feat in cached_definitions
    ])
    assert cached_definitions == [
        CachedDefinitionWithTable(
            feature_id=None,
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
            table_name=cached_definitions[0].table_name,
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[1].feature_id,
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
            table_name=cached_definitions[1].table_name,
        ),
    ]

    more_features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
        ),
    ]
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=document.id,
        feature_definitions=more_features,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id,
    )
    assert all([
        feat.table_name.startswith(MaterializedTableNamePrefix.FEATURE_TABLE_CACHE)
        for feat in cached_definitions
    ])
    assert cached_definitions == [
        CachedDefinitionWithTable(
            feature_id=cached_definitions[0].feature_id,
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
            table_name=cached_definitions[0].table_name,
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[1].feature_id,
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
            table_name=cached_definitions[1].table_name,
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[2].feature_id,
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
            table_name=cached_definitions[2].table_name,
        ),
    ]
    assert all(bool(feat.feature_id) for feat in cached_definitions)


@patch(
    "featurebyte.service.feature_table_cache_metadata.FEATUREBYTE_FEATURE_TABLE_CACHE_MAX_COLUMNS",
    6,
)
@pytest.mark.asyncio
async def test_get_or_create_feature_table_cache_exceed_limit(
    feature_table_cache_metadata_service,
    observation_table_service,
    observation_table,
    mock_snowflake_session,
):
    """
    Test get_or_create_feature_table_cache to create multiple tables when column limit is exceeded
    """
    observation_table_doc = await observation_table_service.create_document(observation_table)

    # Insert definitions to hit the limit
    definitions = [
        CachedFeatureDefinition(
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
        ),
    ]
    cache_metadata = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=len(definitions),
        session=mock_snowflake_session,
    )
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=cache_metadata.id,
        feature_definitions=definitions,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id
    )
    assert cache_metadata.table_name == "FEATURE_TABLE_CACHE_000000000000000000000000"
    assert len(cached_definitions) == 2

    # Insert one
    definitions = [
        CachedFeatureDefinition(
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
        ),
    ]
    cache_metadata = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=len(definitions),
        session=mock_snowflake_session,
    )
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=cache_metadata.id,
        feature_definitions=definitions,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id
    )
    assert cached_definitions == [
        CachedDefinitionWithTable(
            feature_id=cached_definitions[0].feature_id,
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000001",
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[1].feature_id,
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000000",
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[2].feature_id,
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000000",
        ),
    ]

    # Insert one again (still fit in the same table)
    definitions = [
        CachedFeatureDefinition(
            definition_hash="feature_4_definition_hash",
            feature_name="FEATURE_feature_4_definition_hash",
        ),
    ]
    cache_metadata = await feature_table_cache_metadata_service.get_or_create_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        num_columns_to_insert=len(definitions),
        session=mock_snowflake_session,
    )
    await feature_table_cache_metadata_service.update_feature_table_cache(
        cache_metadata_id=cache_metadata.id,
        feature_definitions=definitions,
    )
    cached_definitions = await feature_table_cache_metadata_service.get_cached_definitions(
        observation_table_id=observation_table_doc.id
    )
    assert cached_definitions == [
        CachedDefinitionWithTable(
            feature_id=cached_definitions[0].feature_id,
            definition_hash="feature_3_definition_hash",
            feature_name="FEATURE_feature_3_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000001",
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[1].feature_id,
            definition_hash="feature_4_definition_hash",
            feature_name="FEATURE_feature_4_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000001",
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[2].feature_id,
            definition_hash="feature_1_definition_hash",
            feature_name="FEATURE_feature_1_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000000",
        ),
        CachedDefinitionWithTable(
            feature_id=cached_definitions[3].feature_id,
            definition_hash="feature_2_definition_hash",
            feature_name="FEATURE_feature_2_definition_hash",
            table_name="FEATURE_TABLE_CACHE_000000000000000000000000",
        ),
    ]
