"""
Test feature table cache service
"""
import pytest

from featurebyte.models.feature_table_cache import FeatureTableCacheModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.feature_table_cache import CachedFeatureDefinition, FeatureTableCacheInfo


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
        name="my_feature_table_cache",
        features=["feature_1", "feature_2"],
    )
    document = await feature_table_cache_service.create_document(data)

    get_document = await feature_table_cache_service.get_document_for_observation_table(
        observation_table_id=observation_table_doc.id,
    )
    assert document == get_document


@pytest.mark.asyncio
async def test_get_feature_table_cache_info(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test get_feature_table_cache_info method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert not info

    data = FeatureTableCacheModel(
        observation_table_id=observation_table_doc.id,
        name="my_feature_table_cache",
        features=["feature_1", "feature_2"],
    )
    await feature_table_cache_service.create_document(data)

    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert info == FeatureTableCacheInfo(
        cache_table_name="my_feature_table_cache",
        cached_feature_names=["feature_1", "feature_2"],
    )


@pytest.mark.asyncio
async def test_update_feature_table_cache_from_scratch(
    feature_table_cache_service,
    observation_table_service,
    observation_table,
):
    """test update_feature_table_cache method"""
    observation_table_doc = await observation_table_service.create_document(observation_table)

    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert not info

    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=[],
    )
    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert info.cache_table_name.startswith("feature_table_cache_")
    assert info == FeatureTableCacheInfo(
        cache_table_name=info.cache_table_name,
        cached_feature_names=[],
    )

    features = [
        CachedFeatureDefinition(
            name="feature_1",
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            name="feature_2",
            definition_hash="feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=features,
    )

    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert info.cache_table_name.startswith("feature_table_cache_")
    assert info == FeatureTableCacheInfo(
        cache_table_name=info.cache_table_name,
        cached_feature_names=[
            "feature_1_feature_1_definition_hash",
            "feature_2_feature_2_definition_hash",
        ],
    )


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
            name="feature_1",
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            name="feature_2",
            definition_hash="feature_2_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=features,
    )
    info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert info.cached_feature_names == [
        "feature_1_feature_1_definition_hash",
        "feature_2_feature_2_definition_hash",
    ]

    more_features = [
        CachedFeatureDefinition(
            name="feature_1",
            definition_hash="feature_1_definition_hash",
        ),
        CachedFeatureDefinition(
            name="feature_2",
            definition_hash="feature_2_oops_different_definition_hash",
        ),
        CachedFeatureDefinition(
            name="feature_3",
            definition_hash="feature_3_definition_hash",
        ),
    ]
    await feature_table_cache_service.update_feature_table_cache(
        observation_table_id=observation_table_doc.id,
        feature_definitions=more_features,
    )
    new_info = await feature_table_cache_service.get_feature_table_cache_info(
        observation_table_id=observation_table_doc.id
    )
    assert new_info.cache_table_name == info.cache_table_name
    assert new_info.cached_feature_names == [
        "feature_1_feature_1_definition_hash",
        "feature_2_feature_2_definition_hash",
        "feature_2_feature_2_oops_different_definition_hash",
        "feature_3_feature_3_definition_hash",
    ]
