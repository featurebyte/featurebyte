"""
Test feature list store info related models
"""
import textwrap

import pytest

from featurebyte.models.feature_list_store_info import (
    DataBricksFeatureFunction,
    DataBricksFeatureLookup,
    DataBricksStoreInfo,
)
from featurebyte.query_graph.node.schema import ColumnSpec


@pytest.fixture(name="databricks_store_info")
def databricks_store_info_fixture():
    """
    DataBricks store info fixture
    """
    return DataBricksStoreInfo(
        feature_specs=[
            DataBricksFeatureLookup(
                table_name="ml.feature_table.column1",
                lookup_key=["cust_id"],
                timestamp_lookup_key=None,
                lookback_window=None,
                feature_names=["feature1"],
                rename_outputs={"feature1": "magic_feature1"},
            ),
            DataBricksFeatureFunction(
                udf_name="udf_name",
                input_bindings={"x1": "ml.feature_table.column2"},
                output_name="magic_feature2",
            ),
        ],
        exclude_columns=["column3"],
        require_timestamp_lookup_key=False,
        base_dataframe_specs=[ColumnSpec(name="column1", dtype="INT")],
    )


def test_databricks_feature_specs_definition(databricks_store_info):
    """Test DataBricks feature specs definition"""
    feature_specs = databricks_store_info.feature_specs_definition
    expected = """
    # auto-generated by FeatureByte (based-on databricks-feature-store 0.16.3)
    # Import necessary modules for feature engineering and machine learning
    from databricks.feature_engineering import FeatureEngineeringClient
    from databricks.feature_engineering import FeatureFunction, FeatureLookup
    from pyspark.sql.types import LongType, StructField, StructType
    import mlflow

    # Initialize the Feature Engineering client to interact with Databricks Feature Store
    fe = FeatureEngineeringClient()

    # Define the features for the model
    # FeatureLookup is used to specify how to retrieve features from the feature store
    # Each FeatureLookup or FeatureFunction object defines a set of features to be included
    features = [
        FeatureLookup(
            table_name="ml.feature_table.column1",
            lookup_key=["cust_id"],
            timestamp_lookup_key=None,
            lookback_window=None,
            feature_names=["feature1"],
            rename_outputs={"feature1": "magic_feature1"},
        ),
        FeatureFunction(
            udf_name="udf_name",
            input_bindings={"x1": "ml.feature_table.column2"},
            output_name="magic_feature2",
        ),
    ]

    # List of columns to exclude from the training set
    # Users should consider including request columns and primary entity columns here
    # This is important if these columns are not features but are only needed for lookup purposes
    exclude_columns = ["column3"]

    # Prepare the dataset for log model
    # 'features' is a list of feature lookups to be included in the training set
    # 'exclude_columns' is a list of columns to be excluded from the training set
    target_column = "[TARGET_COLUMN]"
    schema = StructType([StructField("column1", LongType())])
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
    assert feature_specs.strip() == textwrap.dedent(expected).strip()
