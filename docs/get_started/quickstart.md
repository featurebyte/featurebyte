# Quick Start

## Prerequisites

Make sure the Python SDK and local API service are [installed](installation.md#installation).

Test connectivity to the service by listing feature stores

```python
from featurebyte import *

FeatureStore.list()
```

This should return an empty list if you are using the service for the first time.

If this fails refer to the [installation guide](installation.md#troubleshooting) to troubleshoot your service set-up.

## Registering feature stores
Register [feature stores](../about/concepts/feature_store.md) to gain access to data sources and store feature metadata
```python
feature_store = FeatureStore(
    name="Snowflake Demo",
    type="snowflake",
    details=SnowflakeDetails(
        account="xxx.us-central1.gcp",
        warehouse="DEMO",
        database="DEMO",
        sf_schema="FEATUREBYTE",
    )
)
feature_store.save()
```

## Declaring data sources
Declare and annotate [data sources](../about/concepts/data_sources.md) that resides in your data platform to facilitate feature engineering.
```python
credit_card_transactions = EventData.from_tabular_source(
    name="Credit Card Transactions",
    tabular_source=feature_store.get_table(
      database_name="DEMO",
      schema_name="CREDIT_CARD",
      table_name="TRANSACTIONS"
    ),
    event_id_column="TRANSACTIONID",
    event_timestamp_column="TIMESTAMP",
    record_creation_date_column="RECORD_AVAILABLE_AT",
)

credit_card_transactions.save()
```

## Defining features
Define [features](../about/concepts/features.md) using a simple and intuitive Pandas-like interface.
```python
event_view = EventView.from_event_data(
  EventData.get("Credit Card Transactions")
)

recent_spending_features = event_view.groupby("ACCOUNTID").aggregate(
    "AMOUNT",
    method=AggFunc.SUM,
    windows=["7d", "30d"],
    feature_names=["Total spend 7d","Total spend 30d"],
)
```

## Creating feature lists
Create [feature lists](../about/concepts/feature_lists.md) from a group of features for training and serving.
```python
feature_list = FeatureList(
    items=[recent_spending_features],
    name="Credit Card Recent Spending",
)
feature_list.save()
```

## Retrieving Historical Data
Retrieve historical data for training models
```python
training_data = feature_list.get_historical_features(training_examples)
```
