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
The default cleaning specified during the data annotation can be overridden by calling FeatureByte’s pre-built cleaning function with the desired cleaning steps.

If users don’t explicitly call the cleaning function in their SDK script, the default cleaning steps at the time of the feature declaration are automatically attached to the feature to ensure that data is cleaned before the feature is computed and served.

### Flags
FeatureByte automatically suggests flags of data issues based on the critical data info annotated for the selected column.

A built-in function also allows users to define their own flags independently to the critical data info annotated for the columns.

Flags are applied before data cleaning.

### Transforms
To be completed by YungSiang

### Lags
Lags can extract the previous value for a given Entity. This allows the computation of important features such as features based on inter event time and distance from a previous point.

### Joins
To facilitate time aware feature engineering, the event timestamp of the related event data is automatically added to the item view by FeatureByte.

Other joins are recommended when an entity in the view is a primary key or a natural key of another view. 

Joins of Slowly Changing Dimension views are made at the timestamp of the calling view. An offset can be added to the timestamp during the joins. The offset is applied backwards only. 

#### Forward Joins
Forward join of Slowly Changing Dimension views can also be done. Users have to specify the time horizon of the join. The columns added by a forward join have special metadata attached to prevent time leakage and this metadata automatically offsets the time windows of any features using those columns as column input.

### Condition-based Subsetting
Views can be easily filtered in a similar way to Pandas or R Data Frame. Condition based subset can also be used to overwrite the values of a column.

### Change Events
Changes in a Slowly Changing Dimension table may constitute powerful features: how many times has a customer moved in the past 6 months? if he moved in the past 6 months, where did he use to leave? did he get divorced recently? new kids in the family? does he have a new job?

To support such important features, users can create a Change View from a Slowly Changing Dimension table. This new view tracks changes for a given column. The resulting view has 4 columns:

* Change_timestamp: equal to the effective (or start) timestamp of the Slowly Changing Dimension Table,
* the natural key of the Slowly Changing Dimension View,
* value of the column before the change and
* value of the column after the change.

Features can be then created from the view the same way as features from an Event View.
