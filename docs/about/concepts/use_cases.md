A Use Case describes the modeling problem to be solved.

Use cases define the level of analysis (the main Entity), the target, and the context of how features are served: “Churn next 6 months of active customers” or “Fraud detection of transactions before payment”.

Thanks to the Use Case formulation, users are better informed. And when the Use Case is associated with an Event entity, it informs FeatureByte on the need to adapt the serving of features to the context.

In addition to descriptive information, FeatureByte will soon support the mathematical formulation of Use Cases via the formulation of a Context View and a Target Recipe. Thanks to this mathematical formulation, observation sets specifically designed for the Use Case are easily derived for EDA, training, retraining, or test purposes.

### Use Case Main Entities and Parent Entities
The Use Case Main Entities define the level of analysis of the modeling problem.

Use cases are usually associated with one main entity only but can be associated with more than one entity. An example of this is a recommendation use case where two entities are defined: Customer and Product.

Thanks to the Entities Relationship, FeatureByte automatically recommends parent Entities for which Features can be useful for the Use Case.

For a fraud detection use case, although the main Entity is Transaction, powerful features can be also extracted from:

* the Merchant Entity
* the Credit Card Entity
* the Customer Entity
* and the Household Entity

### Context
A context defines the circumstances in which features are expected to be materialized. Examples include:

* an active customer that has made at least one purchase over the past 12 weeks
* a transaction reported as suspicious from its report to its case resolution




#### Context formulation
The minimum information required from users to register a context is as follows:

* entity the context is related to,
* a context name,
* a description

In addition, users can attach an expected inference time and a context view that mathematically defines the context.

The expected inference time can be any time or a scheduled time such as every Monday between 12 and 16 pm.

#### Context View

The Context View defines the periods during which each instance of the context entity is available for materialization. An entity instance can be associated with multiple (non overlapping) periods.

The view has 3 columns: the entity serving key, a start timestamp and an end timestamp. The end timestamp is null if the entity key value is currently subject to materialization (example: Customer active now). The Context View can be created either in the data warehouse or via the SDK. Special operations such as leads (opposite of lags) are allowed in the SDK for a context view.

The Context View can be treated as a Slowly Changing Dimension table to retrieve entity instances available for materialization at any given point-in-time.

It is also used by FeatureByte’s proprietary algorithm to automatically generate observation sets on demand.

#### Context associated with an Event entity
When the context entity is an Event entity, the context information is used to ensure the window end of feature aggregates is prior to the event timestamp to avoid that the event is included in the aggregate. This is critical for use cases such as fraud detection where good features consist of comparing a given transaction with prior transactions.

Further feature engineering is also allowed for contexts associated with an Event entity. Features can be based on an aggregation of events that occurred after the event and prior to the point-in-time of the feature request.

### Target
A target object is created by specifying its name and the entities it is related to. Optionally, users can provide a description, a horizon (window size of forward aggregates or offset from Slowly Changing Dimension table), a blind spot (the time to retrieve the target in addition to the horizon) and a target recipe.

The target recipe is defined from the SDK in a similar way as for features.

The target recipe can be defined directly from Slowly Changing Dimension View. In this case, users specify an offset to define how much time in the future the status should be retrieved. An example of such a target is “marital status in 6 months”.

The target recipe can be more complex and involve Forward Aggregate.

#### Forward Aggregate
The way to define a forward aggregation is very similar to the way windowed aggregations are created from Event Views and Item Views. Instead of specifying a window parameter, users specify a horizon parameter.

If the aggregation is done on a Slowly Changing Dimension View, users specify an offset parameter to define the point-in-time in the future at which the aggregation should be performed. An example of such a target is “Count of Credit Cards held by Customer in 6 months”.
