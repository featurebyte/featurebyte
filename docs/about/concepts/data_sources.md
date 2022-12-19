FeatureByte currently recognizes 4 types of data sources based on the nature of their content:

* Event table
* Item table
* Slowly Changing Dimension table ([type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row))
* Dimension table

New data table types,  Sensor, Time Series data, and Calendars will be supported in the coming releases.

### Connection with data sources
A connection with data sources that reside in cloud data platforms such as Snowflake and DataBricks, is required to serve features for inference.

To allow collaboration with external contributors and facilitate prototyping, our coming release will support csv or parquet snapshots that can be used to run modeling experiments, such as feature list tuning.


### Event table
Event tables (also known as a Transaction Fact Table in Data Warehouses) are a rich source of behavioral features. Each row represents a discrete business event measured at a point in time.

Examples of Event tables include an Order table in E-commerce, Credit Card Transactions in Banking, Doctor Visits in Healthcare, and Clickstream on the Internet.

Examples of common features extracted from Event tables are typical Recency, Frequency and Monetary metrics:

* time since customer’s last order
* count of customer orders in the past 4 weeks
* sum of customer order amounts in the past 4 weeks

More sophisticated features include:

* count of customer visits per weekday the past 12 weeks
* most common weekday in customer visits the past 12 weeks
* weekdays entropy of the past 12 weeks customer visits
* clumpiness of the past 12 weeks customer visits
* weekdays similarity of the past week customer visits with the past 12 weeks visits

Examples of Features extracted for the event entity of the table, such as an Order, include:

* Order amount
* Order amount divided by customer amount averaged over the 12 past weeks
* Order amount z-score based on the past 12 weeks’ customer orders history

### Item table
Data Scientists love Item tables as they provide them with details of business events.

Examples of Item tables include: Product Items purchased in Customer Orders and Drug Prescriptions of Patients Doctor Visits.

An Item table typically has a ‘one to many’ relationship with an Event table. Although the table doesn’t explicitly contain any timestamp, it is implicitly related to an Event timestamp via its relationship with an Event table.

Examples of features extracted from an Item table include:

* amount spent by customer per product type in the past 4 weeks
* customer entropy of amount spent per product type over the past 4 weeks
* similarity of customer’s past week’s basket with her past 12 weeks’ basket
* similarity of customer’s basket with customers living in the same state for the past 4 weeks

While those features are usually considered complex to implement, FeatureByte makes it easy to declare them and serves them into production efficiently.

### Slowly Changing Dimension table
Slowly Changing Dimension tables are also an important source of features for Data Scientists.

A Slowly Changing Dimension table is a table that contains relatively static data that can change slowly but unpredictably. A table of type 2 tracks historical data by creating multiple records for a given natural key. Each natural key instance has at most one active row at a given point in time.

Slowly Changing Dimension tables can be:

* used directly to derive an active status, a count at a given point-in-time or a time-weighted average of balances over a period
* joined to Event tables or Item tables
* or transformed to derive features describing recent changes

Examples of features describing Customer changes in the past 6 months include:

* how many times has the customer moved?
* if she moved, where did she use to live? What is the distance to the active residence?
* does she have a new job?
* what is the time-weighted average of the balance of her bank account?

### Dimension table
A Dimension table is a table that keeps static descriptive information such as a birth date.

Dimension tables can be:

* used directly to derive features for the entity that is the primary key of the table
* joined to Event tables or Item tables

Use of a Dimension table requires special vigilance. If data in a dimension table changes slowly, the table should not be used because those changes can lead to severe data leaks during training and poor performance at inference. In this case, the use of a Slowly Changing Dimension table of type 2 is strongly recommended.

New rows may be added to a dimension table. For this reason, no aggregation is allowed as the addition of new records may lead to Training Serving inconsistencies.

### Data Source Registration
When a new data source is registered, users are required to tag:

* the primary key for Dimension table
* the natural key for Slowly Changing Dimension table, its effective timestamp and its active flag (alternatively the start and end timestamps of the row activity period),
* the event key and timestamp for Event table,
* the item key, the event key and the Event data associated with an Item table,
* the sensor key and timestamp for Sensor data
* the date or timestamp of Time Series and the Time Series key for multi-series 

During an Event table registration, users may also annotate the record creation timestamp. This triggers an automated analysis of the Event data availability and freshness and recommends a default setting for Feature Job scheduling, abstracting the complexity of setting Feature Jobs of features extracted from Event data.
