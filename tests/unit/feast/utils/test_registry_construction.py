"""
Test the construction of the feast register.
"""

import textwrap

import pytest
from google.protobuf.json_format import MessageToDict

from featurebyte import FeatureList, RequestColumn
from featurebyte.common.model_util import get_version
from featurebyte.feast.utils.registry_construction import FeastRegistryBuilder
from tests.util.helper import assert_lists_of_dicts_equal


def test_feast_registry_construction__missing_asset(
    snowflake_feature_store,
    mysql_online_store,
    cust_id_entity,
    float_feature,
    feature_list,
):
    """Test the construction of the feast register (missing asset)"""
    with pytest.raises(ValueError, match="Missing entities: "):
        FeastRegistryBuilder.create(
            feature_store=snowflake_feature_store.cached_model,
            online_store=mysql_online_store.cached_model,
            entities=[],
            features=[float_feature.cached_model],
            feature_lists=[],
            entity_lookup_steps_mapping={},
            serving_entity_ids=None,
        )

    with pytest.raises(ValueError, match="Missing features: "):
        FeastRegistryBuilder.create(
            feature_store=snowflake_feature_store.cached_model,
            online_store=mysql_online_store.cached_model,
            entities=[cust_id_entity.cached_model],
            features=[],
            feature_lists=[feature_list],
            entity_lookup_steps_mapping={},
            serving_entity_ids=None,
        )


@pytest.mark.asyncio
async def test_feast_registry_construction__with_post_processing_features(
    snowflake_feature_store,
    mysql_online_store,
    cust_id_entity,
    transaction_entity,
    float_feature,
    non_time_based_feature,
    latest_event_timestamp_feature,
    mock_pymysql_connect,
    mock_deployment_flow,
    app_container,
    expected_cust_id_via_transaction_id_table_name,
):
    """Test the construction of the feast register (with post processing features)"""
    _ = mock_deployment_flow

    feature_requires_post_processing = (
        (RequestColumn.point_in_time() - latest_event_timestamp_feature).dt.day.cos()
        + float_feature
        + non_time_based_feature
    )
    feature_requires_post_processing.name = "feature"
    feature_requires_post_processing.save()

    feature_list = FeatureList([feature_requires_post_processing], name="test_feature_list")
    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()

    helper_service = app_container.entity_lookup_feature_table_service
    entity_lookup_steps_mapping = await helper_service.get_entity_lookup_steps_mapping([
        feature_list.cached_model
    ])
    feast_registry_proto = FeastRegistryBuilder.create(
        feature_store=snowflake_feature_store.cached_model,
        online_store=mysql_online_store.cached_model,
        entities=[cust_id_entity.cached_model, transaction_entity.cached_model],
        features=[feature_requires_post_processing.cached_model],
        feature_lists=[feature_list.cached_model],  # type: ignore
        entity_lookup_steps_mapping=entity_lookup_steps_mapping,
        serving_entity_ids=feature_list.cached_model.primary_entity_ids,
    )
    feast_registry_dict = MessageToDict(feast_registry_proto)
    on_demand_feature_views = feast_registry_dict["onDemandFeatureViews"]
    assert len(on_demand_feature_views) == 1
    odfv_spec = on_demand_feature_views[0]["spec"]
    assert odfv_spec["name"].startswith("odfv_feature_")
    assert odfv_spec["project"] == "featurebyte_project"
    assert odfv_spec["features"] == [{"name": f"feature_{get_version()}", "valueType": "FLOAT"}]
    assert odfv_spec["sources"].keys() == {
        "POINT_IN_TIME",
        "cat1_transaction_id_1d",
        expected_cust_id_via_transaction_id_table_name,
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
    udf = feast_registry_proto.on_demand_feature_views[
        0
    ].spec.feature_transformation.user_defined_function
    assert udf.body_text.startswith(
        "import datetime\nimport json\nimport numpy as np\nimport pandas as pd\n"
    )

    # check that mock_pymysql_connect was called
    assert mock_pymysql_connect.call_count == 1


@pytest.fixture(name="expected_entity_specs")
def expected_entities_fixture():
    """Fixture for expected entities"""
    return [
        {
            "joinKey": "cust_id",
            "name": "cust_id",
            "project": "featurebyte_project",
            "valueType": "STRING",
        },
        {
            "joinKey": "transaction_id",
            "name": "transaction_id",
            "project": "featurebyte_project",
            "valueType": "STRING",
        },
        {
            "joinKey": "__dummy_id",
            "name": "__dummy",
            "project": "featurebyte_project",
        },
    ]


@pytest.fixture(name="expected_data_sources")
def expected_data_sources_fixture(expected_data_source_names):
    """Fixture for expected data source"""
    expected_data_sources = [
        {
            "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
            "name": data_source_name,
            "project": "featurebyte_project",
            "snowflakeOptions": {
                "database": "sf_database",
                "schema": "sf_schema",
                "table": data_source_name,
            },
            "timestampField": "__feature_timestamp",
            "type": "BATCH_SNOWFLAKE",
        }
        for data_source_name in expected_data_source_names
        if data_source_name != "POINT_IN_TIME"
    ]
    expected_data_sources.append({
        "dataSourceClassType": "feast.data_source.RequestSource",
        "name": "POINT_IN_TIME",
        "project": "featurebyte_project",
        "requestDataOptions": {
            "schema": [{"name": "POINT_IN_TIME", "valueType": "UNIX_TIMESTAMP"}]
        },
        "type": "REQUEST_SOURCE",
    })
    return expected_data_sources


@pytest.fixture(name="expected_feature_view_specs")
def expected_feature_view_specs_fixture(expected_cust_id_via_transaction_id_table_name):
    """Expected feature view specs"""
    common_snowflake_options = {"database": "sf_database", "schema": "sf_schema"}
    common_batch_source = {
        "dataSourceClassType": "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
        "timestampField": "__feature_timestamp",
        "type": "BATCH_SNOWFLAKE",
    }
    common_params = {"project": "featurebyte_project", "online": True}
    version = get_version()
    return [
        {
            **common_params,
            "name": "cat1_transaction_id_1d",
            "batchSource": {
                **common_batch_source,
                "name": "cat1_transaction_id_1d",
                "snowflakeOptions": {
                    **common_snowflake_options,
                    "table": "cat1_transaction_id_1d",
                },
            },
            "entities": ["transaction_id"],
            "entityColumns": [{"name": "transaction_id", "valueType": "STRING"}],
            "features": [
                {
                    "name": f"non_time_time_sum_amount_feature_{version}",
                    "valueType": "FLOAT",
                },
                {
                    "name": f"__composite_feature_ttl_req_col_{version}__part2",
                    "valueType": "FLOAT",
                },
            ],
        },
        {
            **common_params,
            "name": "cat1__no_entity_1d",
            "batchSource": {
                **common_batch_source,
                "name": "cat1__no_entity_1d",
                "snowflakeOptions": {
                    **common_snowflake_options,
                    "table": "cat1__no_entity_1d",
                },
            },
            "entities": ["__dummy"],
            "entityColumns": [{"name": "__dummy_id", "valueType": "STRING"}],
            "features": [
                {"name": "__feature_timestamp", "valueType": "UNIX_TIMESTAMP"},
                {"name": f"count_1d_{version}", "valueType": "INT64"},
            ],
            "ttl": "172800s",
        },
        {
            **common_params,
            "name": "cat1_cust_id_30m",
            "batchSource": {
                **common_batch_source,
                "name": "cat1_cust_id_30m",
                "snowflakeOptions": {
                    **common_snowflake_options,
                    "table": "cat1_cust_id_30m",
                },
            },
            "entities": ["cust_id"],
            "entityColumns": [{"name": "cust_id", "valueType": "STRING"}],
            "features": [
                {"name": "__feature_timestamp", "valueType": "UNIX_TIMESTAMP"},
                {"name": f"sum_1d_{version}", "valueType": "FLOAT"},
                {
                    "name": f"__composite_feature_ttl_req_col_{version}__part0",
                    "valueType": "UNIX_TIMESTAMP",
                },
                {
                    "name": f"__composite_feature_ttl_req_col_{version}__part1",
                    "valueType": "FLOAT",
                },
            ],
            "ttl": "3600s",
        },
        {
            **common_params,
            "name": expected_cust_id_via_transaction_id_table_name,
            "batchSource": {
                **common_batch_source,
                "name": expected_cust_id_via_transaction_id_table_name,
                "snowflakeOptions": {
                    **common_snowflake_options,
                    "table": expected_cust_id_via_transaction_id_table_name,
                },
            },
            "entities": ["transaction_id"],
            "entityColumns": [{"name": "transaction_id", "valueType": "STRING"}],
            "features": [
                {"name": "__feature_timestamp", "valueType": "UNIX_TIMESTAMP"},
                {"name": f"sum_1d_{version}", "valueType": "FLOAT"},
                {
                    "name": f"__composite_feature_ttl_req_col_{version}__part0",
                    "valueType": "UNIX_TIMESTAMP",
                },
                {
                    "name": f"__composite_feature_ttl_req_col_{version}__part1",
                    "valueType": "FLOAT",
                },
            ],
            "ttl": "3600s",
        },
    ]


@pytest.fixture(name="expected_feature_service_spec")
def expected_feature_service_spec_fixture(
    float_feature, feature_without_entity, composite_feature_ttl_req_col
):
    """Expected feature service spec"""
    comp_feat_id = composite_feature_ttl_req_col.id
    version = get_version()
    return [
        {
            "name": f"test_feature_list_{version}",
            "project": "featurebyte_project",
            "features": [
                {
                    "featureColumns": [{"name": f"sum_1d_{version}", "valueType": "FLOAT"}],
                    "featureViewName": f"odfv_sum_1d_{version.lower()}_{float_feature.id}",
                },
                {
                    "featureColumns": [{"name": f"count_1d_{version}", "valueType": "INT64"}],
                    "featureViewName": f"odfv_count_1d_{version.lower()}_{feature_without_entity.id}",
                },
                {
                    "featureColumns": [
                        {
                            "name": f"composite_feature_ttl_req_col_{version}",
                            "valueType": "FLOAT",
                        }
                    ],
                    "featureViewName": f"odfv_composite_feature_ttl_req_col_{version.lower()}_{comp_feat_id}",
                },
                {
                    "featureColumns": [
                        {
                            "name": f"non_time_time_sum_amount_feature_{version}",
                            "valueType": "FLOAT",
                        }
                    ],
                    "featureViewName": "cat1_transaction_id_1d",
                },
            ],
        }
    ]


def test_feast_registry_construction(
    feast_registry_proto,
    expected_entity_specs,
    expected_data_sources,
    expected_feature_view_specs,
    expected_feature_service_spec,
):
    """Test the construction of the feast register"""
    feast_registry_dict = MessageToDict(feast_registry_proto)
    entities = feast_registry_dict["entities"]
    feat_services = feast_registry_dict["featureServices"]
    feat_views = feast_registry_dict["featureViews"]
    on_demand_feature_views = feast_registry_dict["onDemandFeatureViews"]
    feat_view_name = feat_services[0]["spec"]["features"][0]["featureViewName"]
    assert feat_view_name.startswith("odfv_sum_1d_")

    assert len(on_demand_feature_views) == 3
    udf_definition = None
    for odfv in on_demand_feature_views:
        if odfv["spec"]["name"] == feat_view_name:
            udf_definition = odfv["spec"]["featureTransformation"]["userDefinedFunction"][
                "bodyText"
            ]

    expected = f"""
    import datetime
    import json
    import numpy as np
    import pandas as pd
    import scipy as sp


    def {feat_view_name}(
        inputs: pd.DataFrame,
    ) -> pd.DataFrame:
        df = pd.DataFrame()
        # Time-to-live (TTL) handling to clean up expired data
        request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
        cutoff = request_time - pd.Timedelta(seconds=3600)
        feature_timestamp = pd.to_datetime(
            inputs["sum_1d_{get_version()}__ts"], unit="s", utc=True
        )
        mask = (feature_timestamp >= cutoff) & (feature_timestamp <= request_time)
        inputs.loc[~mask, "sum_1d_{get_version()}"] = np.nan
        df["sum_1d_{get_version()}"] = inputs["sum_1d_{get_version()}"]
        df.fillna(np.nan, inplace=True)
        return df
    """
    assert udf_definition.strip() == textwrap.dedent(expected).strip()

    # check that the registry dict is as expected
    assert_lists_of_dicts_equal(feast_registry_dict["dataSources"], expected_data_sources)
    assert_lists_of_dicts_equal(
        [entity["spec"] for entity in entities],
        expected_entity_specs,
    )
    assert_lists_of_dicts_equal(
        [feat_view["spec"] for feat_view in feat_views],
        expected_feature_view_specs,
    )
    assert_lists_of_dicts_equal(
        [feat_service["spec"] for feat_service in feat_services],
        expected_feature_service_spec,
    )
    assert feast_registry_dict["projectMetadata"] == [
        {
            "project": "featurebyte_project",
            "projectUuid": feast_registry_dict["projectMetadata"][0]["projectUuid"],
        }
    ]
