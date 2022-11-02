# Data Source

A data source is a table residing on the data warehouse that is accessible to a feature store.
The nature of a data source dictates the type of features can be defined and how they are defined.

FeatureByte recognizes 4 types of data sources based on the nature of their content.

## Event Data Source
An event data source is a time-series table that captures events over time.
Examples of event data sources are purchase orders and attendance records.

Required column types for event datasource:

| Column Type        | Attribute Name                 | Description                                  |
|--------------------|--------------------------------|----------------------------------------------|
| Event ID           | `event_id_column`              | Event unique identifier                      |
| Event Timestamp    | `event_timestamp_column`       | Time when event occurred                     |
| Creation Timestamp | `record_creation_date_column`  | Time when record is created in the warehouse |

## Item Data Source
An item data source has a foreign key to an associated event data source. There are one or more records associated with each event.

Required column types for item datasource:

| Column Type        | Attribute Name                 | Description                                  |
|--------------------|--------------------------------|----------------------------------------------|
| Event ID           | `event_id_column`              | Event unique identifier                      |
| Creation Timestamp | `record_creation_date_column`  | Time when record is created in the warehouse |

## Slow Changing Dimension (SCD) Data Source

## Dimension Data Source
