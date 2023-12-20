<h1 align="center"> FeatureByte's Declarative Feature Engineering Framework</h1>
<div align="center">

[![Build status](https://github.com/featurebyte/featurebyte/workflows/build/badge.svg?branch=main&event=push)](https://github.com/featurebyte/featurebyte/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/featurebyte.svg)](https://pypi.org/project/featurebyte/)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/featurebyte/featurebyte/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/featurebyte/featurebyte/blob/main/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/featurebyte/featurebyte/releases)
[![License](https://img.shields.io/badge/license-elastic%202.0-yellowgreen)](https://github.com/featurebyte/featurebyte/blob/main/LICENSE)
![Coverage Report](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/kchua78/773e2960183c0a6fe24c644d95d71fdb/raw/coverage.json)

</div>

FeatureByte SDK is the core engine of FeatureByte's [Self-Service Feature Platform](https://featurebyte.com/). It is a **free and source available feature platform** designed to:

* **Create state-of-the-art features, not data pipelines:** Create features for Machine Learning with just a few lines of code. Leave the plumbing and pipelining to FeatureByte. We take care of orchestrating the data ops - whether it’s time-window aggs or backfilling, so you can deliver more value from data.
* **Improve Accuracy through data:** Use the intuitive feature declaration framework to transform creative ideas into training data in minutes. Ditch the limitations of ad-hoc pipelines for features with much more scale, complexity and freshness.
* **Streamline machine learning data pipelines:** Get more value from AI. Faster. Deploy and serve features in minutes, instead of weeks or months. Declare features in Python and automatically generate optimized data pipelines — all using tools you love like Jupyter Notebooks.




## Take charge of the entire ML feature lifecycle

Feature Engineering and management doesn’t have to be complicated. Take charge of the entire ML feature lifecycle. With FeatureByte, you can **create, experiment, serve and manage your features in one tool**.

### Create
- Create and share state-of-the-art ML features effortlessly
- Search and reuse features to create feature lists tailored to your use case

``` python
# Get view from catalog
invoice_view = catalog.get_view("GROCERYINVOICE")
# Declare features of total spent by customer in the past 7 and 28 days
customer_purchases = invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
    "Amount",
    method="sum",
    feature_names=["CustomerTotalSpent_7d", "CustomerTotalSpent_28d"],
    fill_value=0,
    windows=['7d', '28d']
)
customer_purchases.save()
```

### Experiment
- Immediately access historical features through automated backfilling - let FeatureByte handle the complexity of time-aware SQL
- Experiment on live data at scale, innovating faster
- Iterate rapidly with different feature lists to create more accurate models

``` python
# Get feature list from the catalog
feature_list = catalog.get_feature_list(
    "200 Features on Active Customers"
)
# Get an observation set from the catalog
observation_set = catalog.get_observation_table(
    "5M rows of active Customers in 2021-2022"
)
# Compute training data and
# store it in the feature store for reuse and audit
training = \
    feature_list.compute_historical_feature_table(
      observation_set,
      name="Training set to predict purchases next 2w"
    )
```

### Serve
- Deploy AI data pipelines and serve features in minutes
- Access features with low latency
- Reduce costs and security risk by performing computations in your existing data platform
- Ensure data consistency between model training and inferencing

``` python
# Get feature list from the catalog
feature_list = catalog.get_feature_list(
    "200 Features on Active Customers"
)
# Create deployment
deployment = feature_list.deploy(
    name="Features for customer purchases next 2w",
)
# Activate deployment
deployment.enable()
# Get shell script template for online serving
deployment.get_online_serving_code(language="sh")
```

### Manage
- Organize feature engineering assets with domain-specific catalogs
- Centralize cleaning operations and feature job configurations
- Differentiate features that are prototype versus production ready
- Create new versions of your features to handle changes in data
- Keep full lineage of your training data and features in production
- Monitor the health of feature pipelines centrally

``` python
# Get table from catalog
items_table = catalog.get_table("GROCERYITEMS")

# Discount must not be negative
items_table.Discount.update_critical_data_info(
    cleaning_operations=[
        fb.MissingValueImputation(
            imputed_value=0
        ),
        fb.ValueBeyondEndpointImputation(
            type="less_than",
            end_point=0,
            imputed_value=0
        ),
    ]
)
```

Get an [overview of the typical workflow](https://docs.featurebyte.com/latest/about/workflow/) in FeatureByte.

## Get started with Quick-Start and Deep-Dive Tutorials
Discover FeatureByte via its tutorials. All you need is to install the FeatureByte SDK.

Install FeatureByte SDK with pip:
```shell
pip install featurebyte
```
**Note**: To avoid potential conflicts with other packages we strongly recommend using a [virtual environment](https://docs.python.org/3/tutorial/venv.html) or a [conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

Sign up for access to the Hosted Tutorial server [here](https://tutorials.featurebyte.com/tutorial/sign-up) and register your credentials with FeatureByte SDK:
``` python
import featurebyte as fb

# replace <api_token> with your API token you received after registering
fb.register_tutorial_api_token("<api_token>")
```
This will create a "tutorial" profile that uses the hosted tutorial server.
You can now [download](https://docs.featurebyte.com/latest/get_started/tutorials/overview/#download-tutorials) and run notebooks from the [tutorials](https://docs.featurebyte.com/latest/get_started/tutorials/overview/) section.

## Leverage your data warehouse

FeatureByte integrates seamlessly with your **Snowflake, Databricks, or Spark** data warehouses, enhancing security and efficiency by bypassing large-scale outbound data transfers. This integration allows feature calculations to be performed within the data warehouse, leveraging scalability, stability, and efficiency.

<div align="center">
  <img src="https://github.com/featurebyte/featurebyte/blob/main/assets/images/Data%20Warehouse.png" width="600" alt="Warehouse Diagram">
</div>

FeatureByte utilizes your data warehouse as a:

* data source.
* compute engine to leverage its scalability, stability, and efficiency.
* storage of partial aggregates (tiles) and precomputed feature values to support feature serving.

## Architecture

![FeatureByte Architecture](https://github.com/featurebyte/featurebyte/blob/main/assets/images/system_architecture.png)
The FeatureByte platform comprises the following components:
- **FeatureByte SDK** (Python Package): Connects to the API service to provide feature authoring and management functionality through python classes and functions.
- **FeatureByte Service** (Docker Containers):
  - **API Service**: REST-API service that validates and executes requests, queries data warehouses, and stores data.
  - **Worker**: Executes asynchronous or scheduled tasks.
  - **MongoDB**: Store metadata for created assets.
  - **Redis**: Broker and queue for workers, messenger service for publishing progress updates.
- **Query Graph Transpiler** (Python Package): Construct data transformation steps as a query graph, which can be transpiled to platform-specific SQL.
- **Source Tables** (Data Warehouse): Tables used as data sources for feature engineering.
- **Feature Store** (Data Warehouse): Database that store data used to support feature serving.

## FeatureByte Service Deployment Options
The **FeatureByte Service** can be installed in three different modes:

* **Local installation:** The easiest way to get started with the FeatureByte SDK. It is a single-user installation that can be used to prototype features locally with your data warehouse.


* **Hosted on a single server:** A light-weight option to support collaboration and job scheduling with limited scalability and availability. Multiple users can connect to the service using the FeatureByte SDK, and deploy features for production.


* **High availability installation (coming soon):** The recommended way to run the service in production. Scale to a large number of users and deployed features, and provide highly available services.

The FeatureByte Service runs on **Docker** for the first two installation modes, and is deployed on a **Kubernetes Cluster** for the high availability installation mode.

Refer to the [installation](https://docs.featurebyte.com/latest/get_started/installation/) section of the documentation for more details.

## FeatureByte SDK

The FeatureByte Python SDK offers a comprehensive set of objects for feature engineering, simplifying the management and manipulation of tables, entities, views, features, feature lists and other necessary objects for feature serving.

* [**Catalog**](https://docs.featurebyte.com/latest/reference/core/catalog/) objects help you organize your feature engineering assets per domain and maintain clarity and easy access to these assets.
* [**Entity**](https://docs.featurebyte.com/latest/reference/core/entity/) objects contain metadata on entity types represented or referenced by tables within your data warehouse.
* [**Table**](https://docs.featurebyte.com/latest/reference/core/table/) objects centralize key metadata about the table type, important columns, default cleaning operations and default feature job configurations.
* [**View**](https://docs.featurebyte.com/latest/reference/core/view/) objects work like SQL views and are local virtual tables that can be modified and joined to other views to prepare data before feature definition.
* [**Feature**](https://docs.featurebyte.com/latest/reference/core/feature/) objects contain the logical plan to compute a feature in the form of a feature definition file.
* [**FeatureList**](https://docs.featurebyte.com/latest/reference/core/feature_list/) objects are collection of Feature objects tailored to meet the needs of a particular use case.

Refer to the [SDK overview](https://docs.featurebyte.com/latest/about/sdk_overview/) for a complete list of the objects supported by the SDK and the SDK reference for more information about each object.

## Feature Creation

The SDK offers an intuitive declarative framework to create feature objects with different signal types, including timing, regularity, stability, diversity, and similarity in addition to the traditional recency, frequency and monetary types.

### Examples
Features can be as simple as an entity’s attribute:

``` python
customer_view = catalog.get_view("GROCERYCUSTOMER")
# Extract operating system from BrowserUserAgent column
customer_view["OperatingSystemIsWindows"] = \
    customer_view.BrowserUserAgent.str.contains("Windows")
# Create a feature indicating whether the customer is using Windows
uses_windows = customer_view.OperatingSystemIsWindows.as_feature("UsesWindows")
```

Features can be more complex such as aggregations over a window:

``` python
invoice_view = catalog.get_view("GROCERYINVOICE")
# Group invoices by the column GroceryCustomerGuid that references the customer entity
invoices_by_customer = invoice_view.groupby("GroceryCustomerGuid")
# Declare features of total spent by customer over the past 7 days and 28 days
customer_purchases = invoices_by_customer.aggregate_over(
    "Amount",
    method=fb.AggFunc.SUM,
    feature_names=["CustomerTotalSpent_7d", "CustomerTotalSpent_28d"],
    fill_value=0,
    windows=['7d', '28d']
)
```

To capture more complex signals, features can involve a series of joins and aggregates and be derived from multiple features:

``` python
# Get items and product view from the catalog
items_view = catalog.get_view("INVOICEITEMS")
product_view = catalog.get_view("GROCERYPRODUCT")
# Join product view to items view
items_view = items_view.join(product_view)
# Get Customer purchases across product group in the past 4 weeks
customer_basket_28d = items_view.groupby(
    by_keys = "GroceryCustomerGuid", category=”ProductGroup”
).aggregate_over(
   "TotalCost",
    method=fb.AggFunc.SUM,
    feature_names=["CustomerBasket_28d"],
    windows=['28d']
)
# Get customer view and join it to items view
customer_view = catalog.get_view("GROCERYCUSTOMER")
items_view = items_view.join(customer_view)
# Get Purchases of Customers living in the same state
# across product group in the past 4 weeks
state_basket_28d = items_view.groupby(
    by_keys="State", category="ProductGroup"
).aggregate_over(
   "TotalCost",
    method=fb.AggFunc.SUM,
    feature_names=["StateBasket_28d"],
    windows=['28d']
)
# Create a feature that measures the similarity of a customer purchases
# and purchases of customers living in the same state
customer_state_similarity_28d = \
    customer_basket_28d["CustomerBasket_28d"].cd.cosine_similarity(
        state_basket_28d["StateBasket_28d"]
    )
# save the new feature
customer_state_similarity_28d.name = \
    "Customer Similarity with purchases in the same state over 28 days"
customer_state_similarity_28d.save()
```

### Feature Definition
Once a feature is defined, you can obtain its feature definition file that is the source of truth and provides an explicit outline of the intended operations of the feature declaration, including those inherited but not explicitly declared by you.

``` python
customer_state_similarity_28d.definition
```

Refer to the SDK reference for the [Feature](https://docs.featurebyte.com/latest/reference/core/feature/) object for more information.

## Time Travel

Great Machine Learning models require great training data. Great training data require great features (columns) and great observation data points (rows). Great observation data points are historical data points that replicate the production environment. If predictions are expected to be any time, observation points-in-time should also be any time during a period covering at least one seasonal cycle.

Observation data points are in FeatureByte in the form of observation sets that combine entity values and any past points-in-time you want to learn from.

You can choose to get training data as a Pandas DataFrame or as a Table in the feature store that contains metadata on how the table was created for reuse or audit.

``` python
# Get feature list from the catalog
feature_list = catalog.get_feature_list(
    "200 Features on Active Customers"
)
# Create a new feature list that includes a new feature
customer_state_similarity_28d = catalog.get_feature(
    "Customer Similarity with purchases in the same state over 28 days"
)
new_feature_list = fb.FeatureList(
    [feature_list, customer_state_similarity_28d],
    name="Improved feature list with Customer State similarity"
)
# Get an observation set from the catalog
observation_set = catalog.get_observation_table(
    "5M of active Customers in 2021-2022"
)
# Compute training data and store it in the feature store for reuse and audit
training_table = new_feature_list.compute_historical_feature_table(
    observation_set,
    name="Improved Data to predict purchases next 2w with 2021-2022 history"
)
# Download training data as a Pandas DataFrame
training_df = training_table.to_pandas()
```

Refer to the SDK reference for the [FeatureList](https://docs.featurebyte.com/latest/reference/core/feature_list/), [ObservationTable](https://docs.featurebyte.com/latest/reference/core/observation_table/) and [HistoricalFeatureTable](https://docs.featurebyte.com/latest/reference/core/historical_feature_table/) objects, for more information.

## Deploy when needed

Once a feature list is deployed, the FeatureByte Service automatically orchestrates the pre-computation of feature values and stores them in an online feature store for online and batch serving.

``` python
# Get feature list from the catalog
feature_list = catalog.get_feature_list(
    "200 Features on Active Customers"
)
# Check Feature objects are PRODUCTION_READY.
# A readiness metric of 100% should be returned.
print(feature_list.production_ready_fraction)
# Create deployment
my_deployment = feature_list.deploy(
    name="Deployment of 200 Features to predict customers purchases amount next 2 weeks",
)
# Activate deployment
my_deployment.enable()
```

Use the REST API service to retrieve feature values from the online feature store for online serving or use the SDK to retrieve batch of feature values from the online feature store for batch serving.

### Online Serving
For online serving, the Deployment object provides REST API service templates that can be used to serve features. Python or shell script templates for the REST API service are retrieved from the Deployment object.

Get online scoring code as a Python script:
``` python
deployment = catalog.get_deployment(
    "Deployment of 200 Features to predict customers purchases amount next 2 weeks"
)
deployment.get_online_serving_code(language="python")
```

Get online scoring code as a Shell script:
``` python
deployment.get_online_serving_code(language="sh")
```

### Batch Serving
For batch serving, the Deployment object is used to retrieve feature values for a batch request table containing entities values.

``` python
from datetime import datetime
batch_features = deployment.compute_batch_feature_table(
    batch_request_table=batch_request_table,
    batch_feature_table_name=
        "Data to predict customers purchases next 2 w as of " +
        datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
)
# Download batch feature values as a Pandas DataFrame
batch_features_df = batch_features.to_pandas()
```

Refer to the SDK reference for the [Deployment](https://docs.featurebyte.com/latest/reference/core/deployment/), [BatchRequestTable](https://docs.featurebyte.com/latest/reference/core/batch_request_table/) and [BatchFeatureTable](https://docs.featurebyte.com/latest/reference/core/batch_feature_table/) objects, for more information.


## Releases

You can see the list of available releases on the [Change Log](https://github.com/featurebyte/featurebyte/blob/main/CHANGELOG.md) page.
Releases are versioned using the [Semantic Versions](https://semver.org/) specification.

## License

[![License](https://img.shields.io/badge/license-elastic%202.0-yellowgreen)](https://github.com/featurebyte/featurebyte/blob/main/LICENSE)

This project is licensed under the terms of the `Elastic License 2.0` license. See [LICENSE](https://github.com/featurebyte/featurebyte/blob/main/LICENSE) for more details.

## Contributing
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)

All contributions are welcomed. Please adhere to the [Code of Conduct](https://github.com/featurebyte/featurebyte/blob/main/CODE_OF_CONDUCT.md) and read the
[Developer's Guide](https://github.com/featurebyte/featurebyte/blob/main/CONTRIBUTING.md) to get started.
