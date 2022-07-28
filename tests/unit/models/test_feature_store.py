"""
Tests for FeatureStore model
"""
from featurebyte.models.feature_store import FeatureStoreModel


def test_feature_store_model(snowflake_feature_store_model):
    """Test serialization & deserialization of snowflake feature store model"""
    expected_feature_store_dict = {
        "id": snowflake_feature_store_model.id,
        "name": "sf_featurestore",
        "created_at": None,
        "type": "snowflake",
        "details": {
            "account": "account",
            "warehouse": "warehouse",
            "database": "database",
            "sf_schema": "schema",
        },
    }
    assert snowflake_feature_store_model.dict() == expected_feature_store_dict
    feature_store_loaded = FeatureStoreModel.parse_raw(
        snowflake_feature_store_model.json(by_alias=True)
    )
    assert feature_store_loaded == snowflake_feature_store_model
