Features are created in the SDK from Views after data manipulation.

### Lookup features
If the entity is the primary key of the View, a column of this view can be directly converted into a feature for this entity.

If the target entity is the natural key of the Slowly Changing Dimension View, a column of this view can be also directly converted into a feature. In this case, the feature is materialized thanks to point-in-time joins. The value served is by default the row value active as at the point-in-time of the request. Users can specify an offset to retrieve value as at some time prior to the point-in-time of the request (“marital status 6 months ago”). 

### Aggregate features
If the target entity is not the primary (or natural) key of the view, features are defined via aggregates where the entity column is used as the GroupBy key.

For Sensor data, Time Series, Event view and Item view, those aggregates are defined by windows prior to the points in time of the feature request. Windows used in windowed aggregation can be time based or count based. Windows can be offset backwards to allow aggregation of any period of time in the past.

For the Item view, if the target entity is the event key of the view, simple aggregates are applied.

For a Slowly Changing Dimension view and an entity that is not its natural key, the aggregate is applied to records that are active as at the point-in-time of the feature request. Users can specify an offset to retrieve value as at some time prior to the point-in-time of the request (“number of credit cards held by Customer 6 months ago”).

#### Aggregation functions
Examples of aggregation functions supported by FeatureByte include last event, count, sum, mean, max, min, standard deviation, median, quantile and sequence.

To be completed by Sergey

Aggregation per category can also be defined. As an example, a feature can be defined for Customer as the amount spent by Customer per product category the past 4 weeks. In this case, when the feature is materialized for a Customer, a dictionary is returned with as keys the product categories purchased by the Customer and as values, the sum spent for each product category.

### Feature Transforms
A new Feature can be easily derived from existing features. Transforms…

To be completed by Xavier (entropy, cosine similarity…)

### Feature on Demand
Users are able to derive On Demand Features from another feature and request data. An example of a feature on Demand is “Time since Customer’s last order”. In this case, the point-in-time is known only at request time and the “Timestamp of Customer’s last order” is a Customer feature that can be pre-computed.  

### Adding a Feature as a column to a View
Features extracted from other data views can be added as a column to an event view if their entity is present in the view. Then their values can be aggregated as any other column.

This allows the computation of features such as customer average order size the last 3 weeks where order size is a feature extracted from an Item view (Order Details) that counts the number of items per order.

This also allows more complex features such as customer average restaurants 1y ratings visited the last 4 weeks. In this case, the restaurant 1y rating is a windowed aggregation of ratings for each restaurant. To speed up the computation of such complex features, the addition of a windowed aggregation feature requires that the historical values of the added feature are pre-computed and stored in an offline store.
