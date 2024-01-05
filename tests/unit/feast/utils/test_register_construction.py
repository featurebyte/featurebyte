"""
Test the construction of the feast register.
"""
import textwrap

import pytest
from google.protobuf.json_format import MessageToDict

from featurebyte import FeatureList, RequestColumn
from featurebyte.common.model_util import get_version
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


def test_feast_registry_construction__with_post_processing_features(
    snowflake_feature_store,
    cust_id_entity,
    transaction_entity,
    float_feature,
    non_time_based_feature,
    latest_event_timestamp_feature,
):
    """Test the construction of the feast register (with post processing features)"""
    feature_requires_post_processing = (
        (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day.cos()
        + float_feature
        + non_time_based_feature
    )
    feature_requires_post_processing.name = "feature"
    feature_requires_post_processing.save()

    feature_list = FeatureList([feature_requires_post_processing], name="test_feature_list")
    feature_list.save()

    feast_registry_proto = FeastRegistryConstructor.create(
        feature_store=snowflake_feature_store.cached_model,
        entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
        features=[feature_requires_post_processing.cached_model],
        feature_lists=[feature_list.cached_model],  # type: ignore
    )
    feast_registry_dict = MessageToDict(feast_registry_proto)
    on_demand_feature_views = feast_registry_dict["onDemandFeatureViews"]
    assert len(on_demand_feature_views) == 1
    odfv_spec = on_demand_feature_views[0]["spec"]
    assert odfv_spec["name"].startswith("compute_feature_feature_")
    assert odfv_spec["project"] == "featurebyte_project"
    assert odfv_spec["features"] == [{"name": f"feature_{get_version()}", "valueType": "DOUBLE"}]
    assert odfv_spec["sources"].keys() == {
        "POINT_IN_TIME",
        "fb_entity_cust_id_fjs_1800_300_600_ttl",
        "fb_entity_transaction_id",
    }

    data_sources = feast_registry_dict["dataSources"]
    pit_data_source = next(
        data_source for data_source in data_sources if data_source["name"] == "POINT_IN_TIME"
    )
    assert pit_data_source == {
        "dataSourceClassType": "feast.data_source.RequestSource",
        "name": "POINT_IN_TIME",
        "project": "featurebyte_project",
        "requestDataOptions": {
            "schema": [{"name": "POINT_IN_TIME", "valueType": "UNIX_TIMESTAMP"}]
        },
        "type": "REQUEST_SOURCE",
    }

    # dill's getsource() does not include the import statements
    udf = feast_registry_proto.on_demand_feature_views[0].spec.user_defined_function
    assert udf.body_text.startswith("import json\nimport numpy as np\nimport pandas as pd\n")


def test_feast_registry_construction(feast_registry_proto):
    """Test the construction of the feast register"""
    feast_registry_dict = MessageToDict(feast_registry_proto)
    entities = feast_registry_dict["entities"]
    feat_services = feast_registry_dict["featureServices"]
    feat_views = feast_registry_dict["featureViews"]
    on_demand_feature_views = feast_registry_dict["onDemandFeatureViews"]
    feat_view_name = feat_services[0]["spec"]["features"][0]["featureViewName"]
    assert feat_view_name.startswith("compute_feature_sum_1d_")

    assert len(on_demand_feature_views) == 1
    udf_definition = on_demand_feature_views[0]["spec"]["userDefinedFunction"]["bodyText"]
    expected = f"""
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def {feat_view_name}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(inputs["__feature_timestamp"], utc=True)
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        inputs["sum_1d_{get_version()}"][~mask] = np.nan
        df["sum_1d_{get_version()}"] = inputs["sum_1d_{get_version()}"]
        return df
    """
    assert udf_definition.strip() == textwrap.dedent(expected).strip()

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
                "timestampField": "__feature_timestamp",
                "type": "BATCH_SNOWFLAKE",
            },
            {
                "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
                "name": "fb_entity_transaction_id_fjs_86400_0_0",
                "project": "featurebyte_project",
                "snowflakeOptions": {
                    "database": "sf_database",
                    "schema": "sf_schema",
                    "table": "fb_entity_transaction_id_fjs_86400_0_0",
                },
                "timestampField": "__feature_timestamp",
                "type": "BATCH_SNOWFLAKE",
            },
            {
                "dataSourceClassType": "feast.data_source.RequestSource",
                "name": "POINT_IN_TIME",
                "project": "featurebyte_project",
                "requestDataOptions": {
                    "schema": [{"name": "POINT_IN_TIME", "valueType": "UNIX_TIMESTAMP"}]
                },
                "type": "REQUEST_SOURCE",
            },
        ],
        "entities": [
            {
                "meta": {
                    "createdTimestamp": entities[0]["meta"]["createdTimestamp"],
                    "lastUpdatedTimestamp": entities[0]["meta"]["lastUpdatedTimestamp"],
                },
                "spec": {
                    "joinKey": "cust_id",
                    "name": "cust_id",
                    "project": "featurebyte_project",
                    "valueType": "STRING",
                },
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
                    "valueType": "STRING",
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
                            "featureColumns": [
                                {"name": f"sum_1d_{get_version()}", "valueType": "DOUBLE"}
                            ],
                            "featureViewName": feat_view_name,
                        },
                        {
                            "featureColumns": [
                                {
                                    "name": f"non_time_time_sum_amount_feature_{get_version()}",
                                    "valueType": "DOUBLE",
                                }
                            ],
                            "featureViewName": "fb_entity_transaction_id_fjs_86400_0_0",
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
                        "timestampField": "__feature_timestamp",
                        "type": "BATCH_SNOWFLAKE",
                    },
                    "entities": ["cust_id"],
                    "entityColumns": [{"name": "cust_id", "valueType": "STRING"}],
                    "features": [
                        {"name": "__feature_timestamp", "valueType": "UNIX_TIMESTAMP"},
                        {"name": f"sum_1d_{get_version()}", "valueType": "DOUBLE"},
                    ],
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
                        "name": "fb_entity_transaction_id_fjs_86400_0_0",
                        "snowflakeOptions": {
                            "database": "sf_database",
                            "schema": "sf_schema",
                            "table": "fb_entity_transaction_id_fjs_86400_0_0",
                        },
                        "timestampField": "__feature_timestamp",
                        "type": "BATCH_SNOWFLAKE",
                    },
                    "entities": ["transaction_id"],
                    "entityColumns": [{"name": "transaction_id", "valueType": "STRING"}],
                    "features": [
                        {
                            "name": f"non_time_time_sum_amount_feature_{get_version()}",
                            "valueType": "DOUBLE",
                        }
                    ],
                    "name": "fb_entity_transaction_id_fjs_86400_0_0",
                    "online": True,
                    "project": "featurebyte_project",
                },
            },
        ],
        "onDemandFeatureViews": on_demand_feature_views,
        "lastUpdated": feast_registry_dict["lastUpdated"],
        "projectMetadata": [
            {
                "project": "featurebyte_project",
                "projectUuid": feast_registry_dict["projectMetadata"][0]["projectUuid"],
            }
        ],
        "versionId": feast_registry_dict["versionId"],
    }
