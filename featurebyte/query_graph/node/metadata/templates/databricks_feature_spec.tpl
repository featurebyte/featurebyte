# auto-generated by FeatureByte (based-on databricks-feature-store {{databricks_sdk_version}})
# Import necessary modules for feature engineering and machine learning
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering import FeatureFunction, FeatureLookup
{{pyspark_import_statement}}
from sklearn import linear_model
import featurebyte as fb
import mlflow

# Initialize the Feature Engineering client to interact with Databricks Feature Store
fe = FeatureEngineeringClient()

{% if require_timestamp_lookup_key -%}
# Timestamp column name used to retrieve the latest feature values
timestamp_lookup_key = "POINT_IN_TIME"

{% endif -%}
# Define the features for the model
# FeatureLookup is used to specify how to retrieve features from the feature store
# Each FeatureLookup or FeatureFunction object defines a set of features to be included
features = {{features}}

# List of columns to exclude from the training set
# Users should consider including request columns and primary entity columns here
# This is important if these columns are not features but are only needed for lookup purposes
exclude_columns = {{exclude_columns}}

# Prepare the dataset for log model
# 'features' is a list of feature lookups to be included in the training set
# '[TARGET_COLUMN]' should be replaced with the actual name of the target column
# 'exclude_columns' is a list of columns to be excluded from the training set
schema = {{schema}}
log_model_dataset = fe.create_training_set(
    df=spark.createDataFrame([], schema),
    feature_lookups=features,
    label="[TARGET_COLUMN]",
    exclude_columns=exclude_columns,
)

# Retrieve the training dataframe through FeatureByte's compute_historical_features API
# Observation table should include the primary entity columns, the request columns, and the target column
catalog = fb.activate_and_get_catalog("[CATALOG_NAME]")
feature_list = catalog.get_feature_list("[FEATURE_LIST_NAME]")
observation_table = catalog.get_observation_table("[OBSERVATION_TABLE_NAME]")
training_df = feature_list.compute_historical_features(
    observation_set=observation_table.to_pandas(),
)

# Separate the features (X_train) and the target variable (y_train) for model training
# '[TARGET_COLUMN]' should be replaced with the actual name of the target column
X_train = training_df.drop(["[TARGET_COLUMN]"], axis=1)
y_train = training_df.label

# Create and train the linear regression model using the training data
model = linear_model.LinearRegression().fit(X_train, y_train)

# Log the model and register it to the unity catalog
mlflow.set_registry_uri("databricks-uc")

fe.log_model(
    model=model,
    artifact_path="main.default.model",
    flavor=mlflow.sklearn,
    training_set=log_model_dataset,
    registered_model_name="main.default.recommender_model"
)
