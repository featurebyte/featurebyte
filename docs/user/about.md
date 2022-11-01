# What is FeatureByte

FeatureBytes is a suite of tools that makes it easy to define, deploy, share and maintain features for Machine Learning applications using data sources that resides in scalable cloud data platforms such as Snowflake and Databricks.

## Registering feature stores
A Feature Store is a database residing on a data platform that is created specifically for storing feature metadata.

Credentials used to connect to the store must be granted:

- write access to this database
- read access to any database, schema and table on the same platform to be used.

Register feature stores to gain access to data sources and store feature metadata
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
Declare and annotate data sources that resides in your data platform to facilitate feature engineering.
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
Define features using a simple and intuitive Pandas-like interface.
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
Create feature lists from a group of features for training and serving.
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
