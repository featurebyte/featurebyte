Features are created in the SDK from Views after optional data manipulation.

### Lookup features
If the entity is the primary key of the View, a column of this view can be directly converted into a feature for this entity. Examples of such features are 

* Customer’s place of birth
* Transaction’s amount.

If the unit of analysis of the feature is the natural key of a Slowly Changing Dimension View, a column of this view can be also directly converted into a feature. In this case, the feature is materialized thanks to point-in-time joins. The value served is by default the row value active as at the point-in-time of the request. An example of such features is

* Customer’s marital status (at the point in time of the request).

Users can specify an offset to retrieve value as at some time (for example: 6 months) prior to the point-in-time of the request. In this case, the feature will be:

* Customer’s marital status 6 months ago (at the point in time of the request).

### Aggregate features
If the target entity is not the primary (or natural) key of the view, features are defined via aggregates where an entity column is used as the GroupBy key.

For Sensor data, Time Series, Event view and Item view, those aggregates are defined by windows prior to the points in time of the feature request. Windows used in windowed aggregation are time based. Count based windows will be supported in the coming releases. Simple examples of such features are:

* Customer sum of order amounts the past 12 weeks.
* Customer amounts sum of last 5 orders.

In the coming releases, windows can be offset backwards to allow aggregation of any period of time in the past. An example of such feature would be:

* Customer sum of order amounts from 12 weeks ago to 4 weeks ago.

For the Item view, if the target entity is the event key of the view, simple aggregates are applied. An example of such features is:

* Count of items in Order.

For a Slowly Changing Dimension view, 3 types of aggregates will be soon supported:

* Aggregates “as at” a point-in-time
* Time-weighted aggregates over a window
* Aggregates of changes over a window

For a Slowly Changing Dimension view and an entity that is not its natural key, the aggregate will be applied to records that are active as at the point-in-time of the feature request. An example of such features is:

* Number of credit cards held by Customer (at the point in time of the request).

Users will be able to specify an offset to retrieve value as at some time (6 months) prior to the point-in-time of the request. In this case, the feature will be:

* Number of credit cards held by Customer 6 months ago (at the point in time of the request).

For a Slowly Changing Dimension view and an entity that is its natural key, the aggregate will be time-weighted. An example of such features is:

* Time-weighted average of Account balances over the past 4 weeks.

To create features from aggregates on changes, users will be able to create a Change View from a Slowly Changing Dimension table. Then aggregate operations are similar to an Event View. An example of such features is:

* Number of reported insurance claims over the past 12 weeks.

#### Aggregation functions
Examples of aggregation functions supported by FeatureByte include last event, count, na_count, sum, mean, max, min and standard deviation. Sequence functions will be supported in the coming releases.

Aggregation per category can also be defined. As an example, a feature can be defined for Customer as the amount spent by Customer per product category the past 4 weeks. In this case, when the feature is materialized for a Customer, a dictionary is returned with the keys being the product categories purchased by the Customer and the values being the sum spent for each product category.

### Feature Transforms
Features can be transformed in a similar way as columns in a View.

Additional transforms are supported to transform features resulting from an aggregation per category where the feature instance is a dictionary:

* `most_frequent`: Most frequent key
* `unique_count`: Unique number keys
* `get`: Value for a given key
* `entropy`: Entropy over the keys
* `cosine_similarity`: Cosine similarity between two Feature dictionaries

Examples of features resulting from those Feature transforms include:

* most common weekday in customer visits the past 12 weeks
* count of unique items purchased by customer the past 4 weeks
* list of unique items purchased by customer the past 4 weeks
* amount spent by customer in icecream the past 4 weeks
* weekdays entropy of the past 12 weeks customer visits

A Feature can be derived from multiple features when their entities are Child-Parent relationship or when the GroupBy column of the second feature is a categorical attribute of the entity of the first feature. Examples of such features include:

* Similarity of customer past week basket with her past 12 weeks basket
* Similarity of customer item basket with basket of customers in the same city the past 2 weeks
* Order amount z-score based on the past 12 weeks customer orders history

### Feature on Demand
In the coming releases, users will be able to derive On Demand Features from another feature and request data. An example of a Feature on Demand is “Time since Customer’s last order”. In this case, the point-in-time is known only at request time and the “Timestamp of Customer’s last order” is a Customer feature that can be pre-computed. 

### Adding a Feature as a column to a View
Features extracted from other data views can be added as a column to an event view if their entity is present in the view. Then their values can be aggregated as any other column.

This allows the computation of features such as customer average order size the last 3 weeks where order size is itself a feature extracted from an Item view (Order Details).

In the coming releases, this will also allow more complex features such as customer average restaurants 1y ratings visited the last 4 weeks. In this case, the restaurant 1y rating is a windowed aggregation of ratings for each restaurant. To speed up the computation of such complex features, the addition of a windowed aggregation feature requires that the historical values of the added feature are pre-computed and stored in an offline store.
### Features of an Entity Supertype
An Entity Supertype will soon inherit the features of its subtypes. The features will be served without explicitly specifying the subtype serving name. Only the supertype serving name will be required at serving time.

### Features of an Entity Subtype
Features from the Supertype (or another subtype of the supertype) can’t be used directly by the Entity Subtype. Capabilities will be soon built to convert them easily however.
