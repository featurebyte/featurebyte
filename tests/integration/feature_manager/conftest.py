import pytest_asyncio
from bson import ObjectId

from featurebyte.feature_manager.model import ExtendedFeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.query_graph.node.schema import TableDetails


@pytest_asyncio.fixture
async def extended_feature_model(feature_model_dict, session, feature_store):
    """
    Fixture for a ExtendedFeatureModel object
    """

    # this fixture was written to work for snowflake only
    assert session.source_type == "snowflake"

    feature_model_dict.update(
        {
            "tabular_source": {
                "feature_store_id": feature_store.id,
                "table_details": TableDetails(table_name="some_random_table"),
            },
            "version": "v1",
            "readiness": FeatureReadiness.DRAFT,
            "online_enabled": False,
            "table_ids": [
                ObjectId("626bccb9697a12204fb22ea3"),
                ObjectId("726bccb9697a12204fb22ea3"),
            ],
        }
    )
    feature = ExtendedFeatureModel(**feature_model_dict)
    tile_id = feature.tile_specs[0].tile_id

    yield feature

    await session.execute_query("DELETE FROM TILE_REGISTRY")
    await session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_ONLINE")
    await session.execute_query(f"DROP TASK IF EXISTS SHELL_TASK_{tile_id}_OFFLINE")
