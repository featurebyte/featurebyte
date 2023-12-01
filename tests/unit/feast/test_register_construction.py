"""
Test the construction of the feast register.
"""
import pytest
from google.protobuf.json_format import MessageToDict

from featurebyte import FeatureList
from featurebyte.feast.registry_construction import FeastRegistryConstructor


@pytest.fixture(name="feature_list")
def feature_list_fixture(float_feature, non_time_based_feature):
    """Fixture for the feature list"""
    float_feature.save()
    feature_list = FeatureList([float_feature, non_time_based_feature], name="test_feature_list")
    feature_list.save()
    return feature_list


def test_feast_registry_construction__missing_asset(
    snowflake_feature_store,
    cust_id_entity,
    float_feature,
    feature_list,
):
    """Test the construction of the feast register (missing asset)"""
    with pytest.raises(ValueError, match="Missing entities: "):
        FeastRegistryConstructor.create(
            feature_store=snowflake_feature_store.cached_model,
            entities=[],
            features=[float_feature.cached_model],
            feature_lists=[],
        )

    with pytest.raises(ValueError, match="Missing features: "):
        FeastRegistryConstructor.create(
            feature_store=snowflake_feature_store.cached_model,
            entities=[cust_id_entity.cached_model],
            features=[],
            feature_lists=[feature_list],
        )


def test_feast_registry_construction__feast_feature_name_not_found(
    snowflake_feature_store,
    cust_id_entity,
    transaction_entity,
    float_feature,
    non_time_based_feature,
):
    """Test the construction of the feast register (feast feature name not found)"""
    feature_requires_post_processing = float_feature + non_time_based_feature
    feature_requires_post_processing.name = "feature"
    feature_requires_post_processing.save()

    feature_list = FeatureList([feature_requires_post_processing], name="test_feature_list")
    feature_list.save()

    with pytest.raises(ValueError, match="Missing features: {'feature'}"):
        FeastRegistryConstructor.create(
            feature_store=snowflake_feature_store.cached_model,
            entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
            features=[feature_requires_post_processing.cached_model],
            feature_lists=[feature_list.cached_model],  # type: ignore
        )


def test_feast_registry_construction(
    snowflake_feature_store,
    cust_id_entity,
    transaction_entity,
    float_feature,
    non_time_based_feature,
    feature_list,
):
    """Test the construction of the feast register"""
    feast_registry_proto = FeastRegistryConstructor.create(
        feature_store=snowflake_feature_store.cached_model,
        entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
        features=[float_feature.cached_model, non_time_based_feature.cached_model],
        feature_lists=[feature_list],
    )

    feast_registry_dict = MessageToDict(feast_registry_proto)
    assert feast_registry_dict == {
        "dataSources": [
            {
                "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                "snowflakeOptions": {
                    "database": "sf_database",
                    "schema": "sf_schema",
                    "table": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                },
                "type": "BATCH_SNOWFLAKE",
            },
            {
                "name": "fb_entity_transaction_id",
                "snowflakeOptions": {
                    "database": "sf_database",
                    "schema": "sf_schema",
                    "table": "fb_entity_transaction_id",
                },
                "type": "BATCH_SNOWFLAKE",
            },
        ],
        "entities": [
            {"meta": {}, "spec": {"joinKey": "cust_id", "name": "cust_id"}},
            {"meta": {}, "spec": {"joinKey": "transaction_id", "name": "transaction_id"}},
        ],
        "featureServices": [
            {
                "meta": {},
                "spec": {
                    "features": [
                        {
                            "featureColumns": [{"name": "sum_1d", "valueType": "FLOAT"}],
                            "featureViewName": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                        },
                        {
                            "featureColumns": [
                                {"name": "non_time_time_sum_amount_feature", "valueType": "FLOAT"}
                            ],
                            "featureViewName": "fb_entity_transaction_id",
                        },
                    ],
                    "name": "test_feature_list",
                },
            }
        ],
        "featureViews": [
            {
                "meta": {},
                "spec": {
                    "batchSource": {
                        "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
                        "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                        "snowflakeOptions": {
                            "database": "sf_database",
                            "schema": "sf_schema",
                            "table": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                        },
                        "type": "BATCH_SNOWFLAKE",
                    },
                    "entities": ["cust_id"],
                    "features": [{"name": "sum_1d", "valueType": "FLOAT"}],
                    "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                    "online": True,
                    "ttl": "3600s",
                },
            },
            {
                "meta": {},
                "spec": {
                    "batchSource": {
                        "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
                        "name": "fb_entity_transaction_id",
                        "snowflakeOptions": {
                            "database": "sf_database",
                            "schema": "sf_schema",
                            "table": "fb_entity_transaction_id",
                        },
                        "type": "BATCH_SNOWFLAKE",
                    },
                    "entities": ["transaction_id"],
                    "features": [
                        {"name": "non_time_time_sum_amount_feature", "valueType": "FLOAT"}
                    ],
                    "name": "fb_entity_transaction_id",
                    "online": True,
                },
            },
        ],
    }
