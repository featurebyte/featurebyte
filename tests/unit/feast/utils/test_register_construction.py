"""
Test the construction of the feast register.
"""
import pytest
from google.protobuf.json_format import MessageToDict

from featurebyte import FeatureList
from featurebyte.feast.utils.registry_construction import FeastRegistryConstructor


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


def test_feast_registry_construction(feast_registry_proto):
    """Test the construction of the feast register"""
    feast_registry_dict = MessageToDict(feast_registry_proto)
    entities = feast_registry_dict["entities"]
    feat_services = feast_registry_dict["featureServices"]
    feat_views = feast_registry_dict["featureViews"]
    assert feast_registry_dict == {
        "dataSources": [
            {
                "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
                "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                "project": "featurebyte_project",
                "snowflakeOptions": {
                    "database": "sf_database",
                    "schema": "sf_schema",
                    "table": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                },
                "type": "BATCH_SNOWFLAKE",
            },
            {
                "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
                "name": "fb_entity_transaction_id",
                "project": "featurebyte_project",
                "snowflakeOptions": {
                    "database": "sf_database",
                    "schema": "sf_schema",
                    "table": "fb_entity_transaction_id",
                },
                "type": "BATCH_SNOWFLAKE",
            },
        ],
        "entities": [
            {
                "meta": {
                    "createdTimestamp": entities[0]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": entities[0]["meta"]["lastUpdatedTimestamp"],
                },
                "spec": {"joinKey": "cust_id", "name": "cust_id", "project": "featurebyte_project"},
            },
            {
                "meta": {
                    "createdTimestamp": entities[1]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": entities[1]["meta"]["lastUpdatedTimestamp"],
                },
                "spec": {
                    "joinKey": "transaction_id",
                    "name": "transaction_id",
                    "project": "featurebyte_project",
                },
            },
        ],
        "featureServices": [
            {
                "meta": {
                    "createdTimestamp": feat_services[0]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": feat_services[0]["meta"]["lastUpdatedTimestamp"],
                },
                "spec": {
                    "features": [
                        {
                            "featureColumns": [{"name": "sum_1d", "valueType": "DOUBLE"}],
                            "featureViewName": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                        },
                        {
                            "featureColumns": [
                                {"name": "non_time_time_sum_amount_feature", "valueType": "DOUBLE"}
                            ],
                            "featureViewName": "fb_entity_transaction_id",
                        },
                    ],
                    "name": "test_feature_list",
                    "project": "featurebyte_project",
                },
            }
        ],
        "featureViews": [
            {
                "meta": {
                    "createdTimestamp": feat_views[0]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": feat_views[0]["meta"]["lastUpdatedTimestamp"],
                },
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
                    "features": [{"name": "sum_1d", "valueType": "DOUBLE"}],
                    "name": "fb_entity_cust_id_fjs_1800_300_600_ttl",
                    "online": True,
                    "project": "featurebyte_project",
                    "ttl": "3600s",
                },
            },
            {
                "meta": {
                    "createdTimestamp": feat_views[1]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": feat_views[1]["meta"]["lastUpdatedTimestamp"],
                },
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
                        {"name": "non_time_time_sum_amount_feature", "valueType": "DOUBLE"}
                    ],
                    "name": "fb_entity_transaction_id",
                    "online": True,
                    "project": "featurebyte_project",
                },
            },
        ],
        "lastUpdated": feast_registry_dict["lastUpdated"],
        "projectMetadata": [
            {
                "project": "featurebyte_project",
                "projectUuid": feast_registry_dict["projectMetadata"][0]["projectUuid"],
            }
        ],
        "versionId": feast_registry_dict["versionId"],
    }
