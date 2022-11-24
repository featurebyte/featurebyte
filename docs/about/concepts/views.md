Data manipulation in FeatureByte is declared in a similar syntax as Pandas, making the learning curve less steep.

Unlike Pandas, FeatureByte's data transformations follow a lazy execution strategy and are translated as a graphical representation of intended operations. The execution graph is then converted into platform-specific SQL (e.g. SnowSQL or SparkSQL). The transformations are executed only when their values are needed: when a preview or a feature request is performed.

In FeatureByte, transformations are applied to a View Object where:

* cleaning can be specified;
* new columns can be derived;
* lags can be extracted;
* other views can be joined;
* views can be subsetted;
* columns can be edited via conditional statements;
* and changes in a slowly changing dimension table can be converted into an event view.

Additional transformations planned in the coming releases include:

* conversion of event views into time series,
* time series aggregations

### Cleaning
Views are automatically cleaned based on the information collected during the data annotation.

Users can override the default cleaning by applying the desired cleaning steps to the raw data.

### Flags
Flags are automatically created based on the information collected during the data annotation.

Users can easily create additional flags from the raw data.

### Transforms

The following transforms can be applied on columns in a View. That returns a new column that can be assigned back to the View or be used for further transformations. Some transforms are only available to certain data types.

##### Generic
The following transforms are available for columns of all data types:

* `isnull`: Get a new boolean column indicating whether each row is missing
* `notnull`: Get a new boolean column indicating whether each row is non-missing
* `fillna`: Fill missing value in-place
* `astype`: Convert the data type

##### Numeric
The following transforms are available for a numeric column and returns a new column:

* built-in arithmetic operators (`+`, `-`, `*`, `/`, etc)
* `abs`: Absolute value
* `sqrt`: Square root
* `pow`: Power
* `log`: Logarithm with natural base
* `exp`: Exponential function
* `floor`: Round down to the nearest integer
* `ceil`: Round up to the nearest integer

##### String
The following transforms are available for a string column and returns a new column:

* `len`: Get the length of the string
* `lower`: Convert all characters to lowercase
* `upper`: Convert all characters to uppercase
* `strip`: Trim white space(s) or a specific character on the left & right string boundaries
* `lstrip`: Trim white space(s) or a specific character on the left string boundaries
* `rstrip`: Trim white space(s) or a specific character on the right string boundaries
* `replace`: Replace substring with a new string
* `pad`: Pad string up to the specified width size
* `contains`: Get a boolean flag column indicating whether each string element contains a target string
* `slice`: Slice substrings for each string element

##### Datetime
The following transforms are available for a datetime column:

* Difference between two datetime columns
* Datetime component extraction
 * `year`
 * `quarter`
 * `month`
 * `week`
 * `day`
 * `day_of_week`
 * `hour`
 * `minute`
 * `second`
* Addition with a time interval to produce a new datetime column

### Lags
Lags can extract the previous value for a given Entity. This allows the computation of important features such as features based on inter- event time and distance from a previous point.

### Joins
To facilitate time aware feature engineering, the event timestamp of the related event data is automatically added to the item view by FeatureByte.

Other joins are recommended when an entity in the view is a primary key or a natural key of another view.

Joins of Slowly Changing Dimension views are made at the timestamp of the calling view. An offset can be added to the timestamp during the joins. The offset is applied backwards only.

#### Forward Joins
Forward join of Slowly Changing Dimension views can also be done. Users have to specify the time horizon of the join. The columns added by a forward join have special metadata attached to prevent time leakage and this metadata automatically offsets the time windows of any features using those columns as column input.

### Condition-based Subsetting
Views can be easily filtered in a similar way to Pandas or R Data Frame. A condition based subset can also be used to overwrite the values of a column.

### Change Events
Changes in a Slowly Changing Dimension table may constitute powerful features: how many times has a customer moved address in the past 6 months? if she moved in the past 6 months, where did she use to live? did she get divorced recently? new kids in the family? does she have a new job?

To support such important features, users can create a Change View from a Slowly Changing Dimension table. This new view tracks changes for a given column. The resulting view has 4 columns:

* Change_timestamp: equal to the effective (or start) timestamp of the Slowly Changing Dimension Table,
* the natural key of the Slowly Changing Dimension View,
* value of the column before the change and
* value of the column after the change.

Features can be then created from the view the same way as features from an Event View.
