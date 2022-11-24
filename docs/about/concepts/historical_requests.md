Historical requests can be made any time after the feature list declaration.

Historical requests are usually made for EDA, training, retraining or test purposes.

### Request data
The request data of historical requests consists of an observation set that specifies historical values of the feature list (main) entities together with historical points in time.
Users may also provide the instance of the parent entities and the context or use case for which the request is made.

If the feature list served includes On Demand features, the request data should also include the information needed to compute the On Demand features.

### Observation sets
When a Use Case is formulated mathematically with a context view and an expected inference time, observation sets specifically designed for the use case can be easily derived.

Users have to provide:

* the use case name (or context name)
* start and end timestamps to define the period of the observation set
* the maximum size desired
* random seed
* for Period based context for which entity is not an event entity, the desired minimum time interval between 2 observations of the same entity value. The default value of this parameter is equal to the target horizon.

When the use case has a target recipe defined, the target is automatically added to the observation set.
