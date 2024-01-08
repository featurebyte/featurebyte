"""
Test feature table cache service
"""
import pytest
from bson import ObjectId

from featurebyte.models.feature_table_cache import CachedFeatureDefinition, FeatureTableCacheModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.common_table import TabularSource


@pytest.fixture(name="observation_table")
def observation_table_fixture(event_table, user):
    """Observation table fixture"""
    request_input = SourceTableRequestInput(source=event_table.tabular_source)
    location = TabularSource(
        **{
            "feature_store_id": event_table.tabular_source.feature_store_id,
            "table_details": {
                "database_name": "fb_database",
                "schema_name": "fb_schema",
                "table_name": "fb_materialized_table",
            },
        }
    )
    return ObservationTableModel(
        name="observation_table_from_source_table",
        location=location,
        request_input=request_input,
        columns_info=[
            {"name": "a", "dtype": "INT"},
            {"name": "b", "dtype": "INT"},
            {"name": "c", "dtype": "INT"},
        ],
        num_rows=1000,
        most_recent_point_in_time="2023-01-15T10:00:00",
        user_id=user.id,
    )


@pytest.mark.asyncio
async def test_get_document_for_observation_table(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test get_document_for_observation_table method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    get_document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert not get_document

    data = FeatureTableCacheModel(
        observation_table_id=observation_table_doc.id,
        table_name="my_feature_table_cache",
        feature_definitions=[
            CachedFeatureDefinition(
                feature_id=ObjectId(),
                definition_hash="feature_hash_1",
            ),
            CachedFeatureDefinition(
                feature_id=ObjectId(),
                definition_hash="feature_hash_2",
            ),
        ],
    )
    document = await feature_table_cache_service.create_document(data)

    get_document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document == get_document


@pytest.mark.asyncio
async def test_update_feature_table_cache_from_scratch(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert not document

    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=[],
    )
    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert document.feature_definitions == []

    features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=features,
    )

    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert document.feature_definitions == [
        CachedFeatureDefinition(
            feature_id=document.feature_definitions[0].feature_id,
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=document.feature_definitions[1].feature_id,
            definition_hash="feature_2_definition_hash",
        ),
    ]


@pytest.mark.asyncio
async def test_update_feature_table_cache_add_features(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=features,
    )
    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert [feat.definition_hash for feat in document.feature_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
    ]

    more_features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_oops_different_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_3_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=more_features,
    )
    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert [feat.definition_hash for feat in document.feature_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
        "feature_2_oops_different_definition_hash",
        "feature_3_definition_hash",
    ]


@pytest.mark.asyncio
async def test_update_feature_table_cache_updates_feature_id(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    features = [
        CachedFeatureDefinition(
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=features,
    )
    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert [feat.definition_hash for feat in document.feature_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
    ]
    assert not document.feature_definitions[0].feature_id
    assert document.feature_definitions[1].feature_id

    more_features = [
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            feature_id=ObjectId(),
            definition_hash="feature_3_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=more_features,
    )
    document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document.observation_table_id == observation_table_doc.id
    assert document.table_name.startswith("feature_table_cache_")
    assert [feat.definition_hash for feat in document.feature_definitions] == [
        "feature_1_definition_hash",
        "feature_2_definition_hash",
        "feature_3_definition_hash",
    ]
    assert all(bool(feat.feature_id) for feat in document.feature_definitions)
