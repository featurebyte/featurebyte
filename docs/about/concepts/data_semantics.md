The identification of the semantics of data fields and their table is a critical step for a successful and trustworthy feature engineering project. Without this knowledge, non meaningful and noisy features are often produced and important signals are missed. Examples of common mistakes and miss opportunities include:

* sum wrongly applied to a measurement of intensity such as a patient temperature from a doctor visits table
* sum, average, max… applied to a weekday column mistaken for a numerical column while count per weekday, most frequent weekday, weekdays entropy, unique count… should have been used.

To better inform users on which feature engineering should be applied, each data source registered in FeatureByte has a semantic layer that captures and accumulates the domain knowledge acquired by users working on the same data. In this layer, data fields semantics are encoded via a data ontology specifically designed for Feature Engineering.

### Data Ontology
FeatureByte’s Ontology has a tree-based structure where each node represents a semantics type with specific feature engineering practices.

The tree has an inheritance property. A child node inherits from the practices of the parent node it is connected to.

The nodes of level 1 represent basic generic semantics types associated with radically different feature engineering practices:

* `Numeric` type
* `Binary` type
* `Categorical` type
* `Date-time` type
* `Text` type
* `Unique Identifier` type

The nodes of level 2 and 3 represent more precise generic semantics for which additional feature engineering is commonly used.

The nodes of level 4 are domain specific.

#### Nodes of the Numeric type

For the numeric type, the nodes of level 2 mostly determine whether:

* sum can be used,
* average can be used or should be weighted,
* and circular statistics should be used

Its nodes of level 2 are:

* `Additive Numeric` type: for which sum aggregation is recommended, in addition to mean, max, min and standard deviation.
* `Semi-Additive Numeric` type: for which sum aggregation is recommended only at a point in time. Examples include an account balance or a product inventory
* `Non Additive Numeric` type: for which mean, max, min and standard deviation are commonly used but sum is excluded.
* `Ratio/Percentage/Mean` type: for which average and standard deviation should be weighted but max and min can be applied directly. Sum is excluded.
* `Ratio Numerator` / `Ratio Denominator`: for which ratio may be derived and sum aggregation and the ratio of their sums are recommended. An example is moving distance and moving time where:
    * The ratio is speed at a given time from which max can be extracted.
    * The sums are travel distance and travel duration.
    * And the ratio of the sums is the average speed
* `Circular` type: for which circular statistics are usually needed. Examples of data fields of a Circular type include Time of a day, Day of a year and Direction.


Examples of nodes of levels 3 connected to `Non Additive Numeric` type include:

* `Measurement of Intensity` (such as temperature, sound frequency, item price, …): for which change from prior value may be derived 
* `Inter Event Time`: for which clumpiness may be applied
* `Longitude / Latitude of a stationary object`: for which distance from previous location may be derived
* `Longitude / Latitude of a moving object`: for which moving distance, moving time, speed, acceleration and direction may be derived

Examples of domain specific nodes of level 4 include:

* `Patient temperature`: for which bucketing such as low, normal, fever may be derived
* `Patient Blood pressure`: for which bucketing such as hypotension, normal, hypertension may be derived
* `Car location`: for which highway may be detected, high acceleration or high deceleration may be flagged
* `Amount`: for which stats crossed with other categorical columns may be applied or daily, weekly, monthly time series may be derived.

#### Nodes of the Categorical type

The nodes of level 2 determine whether the Categorical field is an `Ordinal` type. In this case, mean may be applied in addition to other features commonly extracted from Categorical fields:

* count per category
* most frequent
* unique count
* entropy
* and similarity and stability features

The nodes of level 3 determines whether the Categorical field is a 'Event type'. In this case, data scientits may be encouraged to

* subset data for each event type
* create features from the 'Event type' sequences.

The domain specific nodes of level 4 inform on further feature engineering that may be required:

* for a `Zip Code`: good practice may consist of concatenating the code with the field with `Country` semantics type
* for a `City`: good practice may consist of concatenating the code with the fields with `State` and `Country` semantics type
* for a `ICD-10-CM`: extracting the first 3 symbols may be useful


#### Nodes of the Date-time type

For the Date-time type, it is common practice to extract Year, Month of a year, Day of a month, Day of a week, Hour of a day, Time of a day, Day of a year.

The nodes of level 2 determine whether the timestamp is an `Event Timestamp`. 

The nodes of level 3 determine whether:

* the `Event Timestamp` is a `Measurement Event Timestamp` or a `Business Event Timestamp`.
* the other types are a `Start Date` or an `End Date`.

For `Business Event Timestamp`, data scientists are likely to attempt to extract features that measure:

* the recency with time since last event
* the clumpiness of events (entropy of inter event time)
* how a customer behavior compares with other customers 
* whether the customer behavior changed overtime

#### Nodes of the Text type

The nodes of level 2 determine whether the Text field is an `Special Text` type or a `Long Text`.

The nodes of level 3 connected to `Special Text` include:

* `Address`
* `URL`
* `Email`
* `Name`
* `Phone Number`
* `Software Code`
* `Lon, Lat`

The nodes of level 3 connected to `Long Text` include:

* `Review`
* `Twitter message`
* `Diagnosis`
* `Product Description`
