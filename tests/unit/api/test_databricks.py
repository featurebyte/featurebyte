import textwrap
from unittest import mock
from unittest.mock import AsyncMock, patch

import freezegun
import pandas as pd
import pytest

from featurebyte import Context, DatabricksDetails, FeatureList, UseCase
from featurebyte.exception import DeploymentDataBricksAccessorError, NotInDataBricksEnvironmentError
from featurebyte.models import FeatureStoreModel
from featurebyte.models.precomputed_lookup_feature_table import get_lookup_steps_unique_identifier


@pytest.fixture(name="always_enable_feast_integration", autouse=True)
def always_enable_feast_integration_fixture(
    patched_catalog_get_create_payload,
    mock_deployment_flow,
):
    """Enable feast integration & patch catalog ID for all tests in this module"""
    _ = patched_catalog_get_create_payload, mock_deployment_flow
    yield


@pytest.fixture(name="databricks_use_case")
def databricks_use_case_fixture(transaction_entity, snowflake_event_view_with_entity):
    """Databricks use case fixture"""
    primary_entity = [transaction_entity.name]
    grouped_event_view = snowflake_event_view_with_entity.groupby("col_int")
    target = grouped_event_view.forward_aggregate(
        method="sum",
        value_column="col_float",
        window="1d",
        target_name="float_target",
        fill_value=0.0,
    )
    target.save()
    context = Context.create(name="transaction_context", primary_entity=primary_entity)
    use_case = UseCase.create(
        name="transaction_use_case",
        target_name=target.name,
        context_name=context.name,
    )
    return use_case


@pytest.fixture(name="another_databricks_use_case")
def another_databricks_use_case_fixture(item_entity, snowflake_item_view_with_entity):
    """Another databricks use case fixture"""
    primary_entity = [item_entity.name]
    target = snowflake_item_view_with_entity.item_amount.as_target("item_amount", fill_value=None)
    target.save()
    context = Context.create(name="item_context", primary_entity=primary_entity)
    use_case = UseCase.create(
        name="item_use_case",
        target_name=target.name,
        context_name=context.name,
    )
    return use_case


@pytest.fixture(name="relative_freq_feature")
def relative_freq_feature_fixture(
    snowflake_event_view_with_entity, snowflake_scd_view, arbitrary_default_feature_job_setting
):
    """Test count dict feature udf"""
    event_view = snowflake_event_view_with_entity
    scd_view = snowflake_scd_view
    joined_view = event_view.join(scd_view, on="col_int", rprefix="scd_")
    feat = joined_view["col_boolean"].as_feature("col_boolean")
    feat1 = event_view.groupby(by_keys=[], category="col_boolean").aggregate_over(
        value_column=None,
        method="count",
        windows=["4w"],
        feature_names=["overall_count_of_col_text"],
        feature_job_setting=arbitrary_default_feature_job_setting,
    )["overall_count_of_col_text"]
    feat_rel_freq = feat1.cd.get_relative_frequency(key=feat)
    feat_rel_freq.name = "relative_frequency"
    return feat_rel_freq


@pytest.fixture(name="count_feature")
def count_feature_fixture(grouped_event_view, feature_group_feature_job_setting):
    """Count feature fixture"""
    grouped = grouped_event_view.aggregate_over(
        value_column=None,
        method="count",
        windows=["1d"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["count_feature"],
    )
    return grouped["count_feature"]


@pytest.fixture(name="databricks_deployments")
def databricks_deployment_fixture(
    float_feature,
    count_feature,
    non_time_based_feature,
    ttl_non_ttl_composite_feature,
    latest_event_timestamp_overall_feature,
    relative_freq_feature,
    req_col_day_diff_feature,
    databricks_use_case,
    another_databricks_use_case,
):
    """Databricks deployment fixture"""
    use_case = databricks_use_case
    features = [
        float_feature,
        count_feature,
        non_time_based_feature,
        ttl_non_ttl_composite_feature,
        req_col_day_diff_feature,
        latest_event_timestamp_overall_feature,
        relative_freq_feature,
    ]
    for feature in features:
        with freezegun.freeze_time("2024-01-03"):
            feature.save()

    feature_list = FeatureList(features, name="feature_list")
    feature_list.save()
    with mock.patch(
        "featurebyte.service.feature_list.FeatureStoreService.get_document", new_callable=AsyncMock
    ) as mock_get_document:
        # mock the feature store service to return the databricks feature store
        feature_store = FeatureStoreModel(
            name="databricks_feature_store",
            type="databricks_unity",
            details=DatabricksDetails(
                host="host.databricks.com",
                http_path="sql/protocalv1/some_path",
                catalog_name="feature_engineering",
                schema_name="some_schema",
                storage_path="dbfs:/FileStore/some_storage_path",
            ),
        )
        mock_get_document.return_value = feature_store
        deployment = feature_list.deploy(
            make_production_ready=True, ignore_guardrails=True, use_case_name=use_case.name
        )
        deployment.enable()

        another_deployment = feature_list.deploy(
            deployment_name="another_deployment",
            make_production_ready=True,
            ignore_guardrails=True,
            use_case_name=another_databricks_use_case.name,
        )
        another_deployment.enable()
        yield deployment, another_deployment


@pytest.fixture(name="cust_id_30m_suffix")
def cust_id_30m_suffix_fixture(databricks_deployments):
    """Offset table suffix fixture"""
    relationships_info = databricks_deployments[0].feature_list.cached_model.relationships_info
    suffix = get_lookup_steps_unique_identifier([
        info for info in relationships_info if info.entity_column_name == "col_int"
    ])
    return suffix


@pytest.fixture(name="mock_is_databricks_env")
def mock_is_databricks_env_fixture():
    """Mock is_databricks_environment"""
    with patch(
        "featurebyte.api.accessor.databricks._is_databricks_environment"
    ) as mock_is_databricks_env:
        yield mock_is_databricks_env


def test_databricks_accessor__with_non_databricks_unity_feature_store(deployment):
    """Test databricks accessor with non-databricks feature store"""
    expected = "Deployment is not enabled"
    with pytest.raises(DeploymentDataBricksAccessorError, match=expected):
        _ = deployment.databricks

    deployment.enable()
    expected = "Deployment is not using DataBricks Unity as the store"
    with pytest.raises(DeploymentDataBricksAccessorError, match=expected):
        _ = deployment.databricks


def test_databricks_specs(
    count_feature,
    ttl_non_ttl_composite_feature,
    req_col_day_diff_feature,
    relative_freq_feature,
    databricks_deployments,
    cust_id_30m_suffix,
):
    """Test databricks specs"""
    expected = """
    # auto-generated by FeatureByte (based-on databricks-feature-store 0.16.3)
    # Import necessary modules for feature engineering and machine learning
    from databricks.feature_engineering import FeatureEngineeringClient
    from databricks.feature_engineering import FeatureFunction, FeatureLookup
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    import mlflow

    # Initialize the Feature Engineering client to interact with Databricks Feature Store
    fe = FeatureEngineeringClient()

    # Timestamp column name used to retrieve the latest feature values
    timestamp_lookup_key = "POINT_IN_TIME"

    # Define the features for the model
    # FeatureLookup is used to specify how to retrieve features from the feature store
    # Each FeatureLookup or FeatureFunction object defines a set of features to be included
    features = [
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_cust_id_30m_via_transaction_id_[TABLE_SUFFIX]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["sum_1d_V240103"],
            rename_outputs={"sum_1d_V240103": "sum_1d"},
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_cust_id_30m_via_transaction_id_[TABLE_SUFFIX]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["count_feature_V240103"],
            rename_outputs={"count_feature_V240103": "count_feature"},
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_transaction_id_1d",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["non_time_time_sum_amount_feature_V240103"],
            rename_outputs={
                "non_time_time_sum_amount_feature_V240103": "non_time_time_sum_amount_feature"
            },
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_cust_id_30m_via_transaction_id_[TABLE_SUFFIX]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["__feature_V240103__part0"],
            rename_outputs={},
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_transaction_id_1d",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["__feature_V240103__part1"],
            rename_outputs={},
        ),
        FeatureFunction(
            udf_name="feature_engineering.some_schema.udf_feature_v240103_[FEATURE_ID1]",
            input_bindings={
                "x_1": "__feature_V240103__part1",
                "x_2": "__feature_V240103__part0",
            },
            output_name="feature",
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_cust_id_30m_via_transaction_id_[TABLE_SUFFIX]",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["__req_col_feature_V240103__part0"],
            rename_outputs={},
        ),
        FeatureFunction(
            udf_name="feature_engineering.some_schema.udf_req_col_feature_v240103_[FEATURE_ID2]",
            input_bindings={
                "x_1": "__req_col_feature_V240103__part0",
                "r_1": "POINT_IN_TIME",
            },
            output_name="req_col_feature",
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1__no_entity_30m",
            lookup_key=["__featurebyte_dummy_entity"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["latest_event_timestamp_overall_90d_V240103"],
            rename_outputs={
                "latest_event_timestamp_overall_90d_V240103": "latest_event_timestamp_overall_90d"
            },
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1_transaction_id_1d",
            lookup_key=["transaction_id"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["__relative_frequency_V240103__part1"],
            rename_outputs={},
        ),
        FeatureLookup(
            table_name="feature_engineering.some_schema.cat1__no_entity_15m",
            lookup_key=["__featurebyte_dummy_entity"],
            timestamp_lookup_key=timestamp_lookup_key,
            lookback_window=None,
            feature_names=["__relative_frequency_V240103__part0"],
            rename_outputs={},
        ),
        FeatureFunction(
            udf_name="feature_engineering.some_schema.udf_relative_frequency_v240103_[FEATURE_ID3]",
            input_bindings={
                "x_1": "__relative_frequency_V240103__part0",
                "x_2": "__relative_frequency_V240103__part1",
            },
            output_name="relative_frequency",
        ),
    ]

    # List of columns to exclude from the training set
    # Users should consider including request columns and primary entity columns here
    # This is important if these columns are not features but are only needed for lookup purposes
    exclude_columns = [
        "POINT_IN_TIME",
        "__feature_V240103__part0",
        "__feature_V240103__part1",
        "__featurebyte_dummy_entity",
        "__relative_frequency_V240103__part0",
        "__relative_frequency_V240103__part1",
        "__req_col_feature_V240103__part0",
        "transaction_id",
    ]

    # Prepare the dataset for log model
    # 'features' is a list of feature lookups to be included in the training set
    # 'exclude_columns' is a list of columns to be excluded from the training set
    target_column = "float_target"
    schema = StructType(
        [
            StructField(target_column, DoubleType()),
            StructField("__featurebyte_dummy_entity", StringType()),
            StructField("transaction_id", LongType()),
            StructField("POINT_IN_TIME", TimestampType()),
        ]
    )
    spark = SparkSession.builder.getOrCreate()
    log_model_dataset = fe.create_training_set(
        df=spark.createDataFrame([], schema),
        feature_lookups=features,
        label=target_column,
        exclude_columns=exclude_columns,
    )

    # Log the model and register it to the unity catalog
    fe.log_model(
        model=model,  # model is the trained model
        artifact_path="[ARTIFACT_PATH]",  # artifact_path is the path to the model
        flavor=mlflow.sklearn,
        training_set=log_model_dataset,
        registered_model_name="[REGISTERED_MODEL_NAME]",  # registered model name in the unity catalog
    )
    """
    relationships_info = databricks_deployments[0].feature_list.cached_model.relationships_info
    assert len(relationships_info) == 2
    replace_pairs = [
        ("[FEATURE_ID0]", str(count_feature.cached_model.id)),
        ("[FEATURE_ID1]", str(ttl_non_ttl_composite_feature.cached_model.id)),
        ("[FEATURE_ID2]", str(req_col_day_diff_feature.cached_model.id)),
        ("[FEATURE_ID3]", str(relative_freq_feature.cached_model.id)),
        ("[TABLE_SUFFIX]", cust_id_30m_suffix),
    ]
    for replace_pair in replace_pairs:
        expected = expected.replace(*replace_pair)

    feat_specs = databricks_deployments[0].databricks.get_feature_specs_definition()
    assert feat_specs.strip() == textwrap.dedent(expected).strip()

    # test skip exclude columns
    feat_specs = databricks_deployments[0].databricks.get_feature_specs_definition(
        skip_exclude_columns=["transaction_id"]
    )
    expected_sub_string = """
    exclude_columns = [
        "POINT_IN_TIME",
        "__feature_V240103__part0",
        "__feature_V240103__part1",
        "__featurebyte_dummy_entity",
        "__relative_frequency_V240103__part0",
        "__relative_frequency_V240103__part1",
        "__req_col_feature_V240103__part0",
    ]
    """
    assert textwrap.dedent(expected_sub_string).strip() in feat_specs

    # check another deployment's feature specs
    another_feature_specs = databricks_deployments[1].databricks.get_feature_specs_definition()
    assert another_feature_specs != feat_specs
    assert 'lookup_key=["item_id"]' in another_feature_specs
    assert 'target_column = "item_amount"' in another_feature_specs


def test_databricks_commands__run_in_non_databricks_env(databricks_deployments):
    """Test databricks commands run in non-databricks environment"""
    expected = "This method can only be called in a DataBricks environment."
    with pytest.raises(NotInDataBricksEnvironmentError, match=expected):
        databricks_deployments[0].databricks.log_model(
            model=None, artifact_path="some_path", flavor=None, registered_model_name="some_name"
        )

    with pytest.raises(NotInDataBricksEnvironmentError, match=expected):
        _ = databricks_deployments[0].databricks.score_batch(
            model_uri="some_uri", df=pd.DataFrame()
        )


def test_databricks_commands__missing_import(mock_is_databricks_env, databricks_deployments):
    """Test databricks commands with missing import"""
    _ = mock_is_databricks_env

    expected = "Please install the databricks feature engineering package to use this accessor."
    with pytest.raises(ImportError, match=expected):
        databricks_deployments[0].databricks.log_model(
            model=None,
            artifact_path="some_path",
            flavor=None,
            registered_model_name="some_name",
        )

    with pytest.raises(ImportError, match=expected):
        _ = databricks_deployments[0].databricks.score_batch(
            model_uri="some_uri", df=pd.DataFrame()
        )


def test_list_feature_table_names(databricks_deployments, cust_id_30m_suffix):
    """Test list feature table names"""
    output = databricks_deployments[0].databricks.list_feature_table_names()
    assert output == [
        "feature_engineering.some_schema.cat1__no_entity_15m",
        "feature_engineering.some_schema.cat1__no_entity_30m",
        f"feature_engineering.some_schema.cat1_cust_id_30m_via_transaction_id_{cust_id_30m_suffix}",
        "feature_engineering.some_schema.cat1_transaction_id_1d",
    ]


def test_no_null_filling_on_count_feature(databricks_deployments, count_feature):
    """Test null filling UDF"""
    udf_info = count_feature.cached_model.offline_store_info.udf_info
    assert udf_info is None
