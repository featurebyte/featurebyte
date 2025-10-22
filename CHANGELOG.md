# Changelog

## v3.3.0 (2025-10-22)

### 💡 Enhancements

+ `service` Add models, services and routes for SnapshotsTable
+ `service` Make positive label immutable once set for target namespace.
  + Positive labels for classification target namespaces cannot be changed after being set
  + Refactored target namespace controller to make positive label validation logic extensible
+ `service` Allow context and use case to be specified during observation table creation.
+ `service` Support creating an observation table from another with sampling options.

### 🐛 Bug Fixes

+ `service` Fix NullFillingValueExtractor to properly handle object dtype metadata for aggregation operations with categories.
  Fixed issue where `get_value` operations on dictionary features with categorical groupby were not preserving object dtype metadata
+ `service` When the event table's entity columns & event timestamp share the exact same name as item table columns, the item view generated from the item table causes error when saving feature that uses the item view.
+ `service` Fix issue when serving parent features with tiles in batch feature requests
+ `service` Fix dtype derivation for count_dict.get_value when the parent aggregation yields an OBJECT dictionary column by normalizing to FLOAT to prevent incorrect dtype propagation.

## v3.2.0 (2025-08-04)

### 💡 Enhancements

+ `service` Batch features will be computed using temporary tile tables by default
+ `api` Use snapshot date as regular primary key instead of timeseries key for external DataBricks feature table by default.
+ `service` Add routes to update compute options for a deployment
+ `service` Allow using larger observation tables for preview by using the first rows within preview row limit.

## v3.1.2 (2025-07-14)

### 💡 Enhancements

+ `service` Implement Development Datasets for faster experimentation.
+ `service` Add datetime partition column and schema fields to event and time series table creation parameters

### 🐛 Bug Fixes

+ `service` Fix inconsistent request timestamp causing missing values in time series batch features

## v3.1.1 (2025-07-14) YANKED

Publishing error

## v3.1.0 (2025-06-28)

### 💡 Enhancements

+ `adapter` Skip sorting during sampling for large datasets (>10M rows) to improve speed.
  + This change is intended to improve sampling speed for large datasets (>10M rows)
  + The default behavior remains unchanged for smaller datasets
  + Sampling for datasets larger than 10M rows will not be deterministic even if seed is provided
+ `service` Support one-to-one relationship type between entities
+ `session` Use python 3.11 runtime for Snowflake UDFs
+ `service` Add option to specify point in time when computing batch feature tables
+ `service` Add service and routes to support managed views that can be used as source tables
+ `service` Reduce cap on number of most frequent categories to keep during aggregation to avoid creating too large feature tables
+ `service` Disable online feature serving if online store is not configured for the catalog
+ `service` Add API and SDK function to compute batch features that are appended to an unmanaged feature table

### 🐛 Bug Fixes

+ `utils` Handle control characters and invalid json strings in dataframe_from_arrow_table helper function
+ `service` Fix boolean features and targets being materialized as string types in the database.
+ `session` Handle session lost in DataBricks connection with retry using re-initialized connection

## v3.0.1 (2025-04-07)

### 💡 Enhancements

+ `session` Support using M2M Oauth for authentication with DataBricks
+ `service` Add support for blind spot in calendar aggregation
+ `session` Remove group_name for setting up DataBricks Unity feature store
+ `service` Add to_timestamp_from_epoch function to convert epoch time to timestamp

## v3.0.0 (2025-04-01)

### 🛑 Breaking Changes

+ `api` Make `fill_value` mandatory argument in `forward_aggregate`, `forward_aggregate_asat` and `as_target`.

### 💡 Enhancements

+ `service` Updates observation table construction logic to store invalidate rows from the observation table.
+ `service` Support timestamp schema for event timestamp column
+ `service` Support cron based default feature job setting for EventTable
+ `service` Introduce feature type to feature namespace.
+ `dependencies` Bump cryptography package >44.0.1
+ `service` fillna operation on a Target object now preserves the original target name automatically
+ `credential` Add support for key-pair authentication for Snowflake feature store
+ `api` Support creating batch feature tables from source tables and views without creating batch request tables.
+ `middleware` Merging ExecutionContext and ExceptionMiddleware functionality
+ `api` Introduce AddTimestampSchema column cleaning operation
+ `target` Introduce a target_type attribute to the target object, allowing explicit specification and updates of target prediction types.
+ `session` Sort database object listings in lexical order

### ⚠️ Deprecations

+ `python` Deprecating python version 3.9

### 🐛 Bug Fixes

+ `service` Fix an error when joining TimeSeriesView with SCDView in Snowflake due to timezone handling
+ `service` Fix feature version number generation bug when a previous version is deleted in the same day
+ `service` Fix grouping of calendar aggregation features when materializing in batch
+ `service` Fix handling of effective timestamp schema when serving parent features and deployment
+ `session` Fix large dates (e.g. 9999-01-01) causing source table preview to fail
+ `service` Fix concurrent materialization of feature lists with overlapping features
+ `service` Fix syntax error due to malformed DATEDIFF expressions
+ `session` Fix fallback calls for table and schema listing when user has not access to catalog INFORMATION_SCHEMA
+ `service` Fix SCD joins to take end timestamp column into account when available

## v2.1.0 (2024-12-20)

### 💡 Enhancements

+ `warehouse_session` SessionManager caching logic is refactored to be in SessionManagerService
  Deprecation of SessionManager in its entirety in favor of SessionManagerService
+ `warehouse_session` Simplification of FeatureStoreController DWH creation logic
+ `route` Add route to preview a feature from a historical feature table.
+ `session` Add support to configure max query concurrency for a feature store
+ `service` Implementing UserGroups for credential management
+ `task_manager` Add support for re-submitting failed jobs in the task manager
+ `service` Add validation for event ID column dtypes between item and event table during item table registration.
+ `warehouse_session` Deprecating of SessionManager instance variables
+ `api` Add overwrite parameter to file download methods in the SDK.
+ `session` Enforce application-wide concurrency limit on long running SQL execution
+ `api` Add support for sampling by time range during registration of observation tables from source table or view.
+ `service` Add data structures, routes and api for creation and manipulation of new Time Series table type
  Feature creation functionality for Time Series table type to be added later
+ `service` Improve tile cache working table query efficiency in BigQuery
+ `warehouse_session` Merging functionality of SessionManagerService and SessionValidatorService
+ `service` Support non-timestamp special columns in SCDTable
+ `session` Cancel the query when interrupted by timeout or asyncio task cancellation.
+ `service` Prevent users to tag multiple columns with the same entity in the same table.
+ `service` Entity tags will be automatically removed when the table is deprecated.
+ `service` Implemented a consistency check to ensure entity data types are consistent between associated tables.

### ⚠️ Deprecations

+ `service` Merging MongodbBackedCredentialProvider with CredentialService

### 🐛 Bug Fixes

+ `service` Fixed an error in historical feature materialization when performing min/max aggregations on timestamp columns.
+ `service` Fixed overflow error for specific UDFs in Databricks SQL Warehouse.
+ `service` Fix computation of complex count distinct aggregation features involving multiple windows
+ `model` Move the `store_info` attribute from feature list model to deployment model.
+ `service` Fix scheduled tile task error due to missing `tile_compute_query`
+ `service` Fixed online serving code using serving entities from feature list instead of deployment.
+ `service` Fix entity untagging bug when removing ancestor IDs that can be reached by multiple paths
+ `service` Fix observation table creation failure when using a datetime object with timezone as sample_from_timestamp or sample_to_timestamp.
  The created observation table is also filtered to exclude rows where POINT_IN_TIME is too recent for | feature materialization, or where mapped columns have NULL values.

## v2.0.1 (2024-09-13)

### 💡 Enhancements

+ `session` Add support for BigQuery as a feature store backend
+ `service` Update ItemTable.get_view() method to auto-resolve column name conflicts by removing the conflicting column from the event table
+ `dependencies` Bump vulnerable dependencies.
  + jupyterlab to `^4.2.5`
  + aiohttp to `^3.10.2`
+ `service` Tighten asset name length validation in the service from 255 to 230 characters
+ `service` Allow some special columns (event_id_column, item_id_column, natural_key_column) to be optional during table registration.
+ `service` Support VARCHAR columns with a maximum length (previously detected as UNKNOWN)

### 🐛 Bug Fixes

+ `websocket` Fixes issue with websocket connection not disconnecting properly

## v2.0.0 (2024-07-31)

### 🛑 Breaking Changes

+ `service` Remove default values for mandatory arguments in aggregation methods such as aggregate_over.
    The value_column parameter must now be provided.

### 💡 Enhancements

+ `linting` Using ruff as the linter for the project
+ `package` Upgrade Pydantic to V2

### ⚠️  Deprecations

+ `dependencies` Deprecation of `sasl` library to support python 3.11

### 🐛 Bug Fixes

+ `service` Fix feature metadata extraction throwing KeyError during feature info retrieval
+ `session` Handle schema and table listing for catalogs without information schema in DataBricks Unity.

## v1.1.4 (2024-07-09)

### 💡 Enhancements

+ `service` Validate aggregation method is supported in more aggregation methods
+ `service` Added support for count_distinct aggregation method

### 🐛 Bug Fixes

+ `numpy` Explicitly Set lower bound for numpy version to <2.0.0

## v1.1.3 (2024-07-05)

### 💡 Enhancements

+ `worker` Speed up table description by excluding top and most frequent values for float and timestamp columns.

### 🐛 Bug Fixes

+ `api` Fix materialized table download failure.


## v1.1.2 (2024-06-25)

### 💡 Enhancements

+ `service` Improve feature job efficiency for latest aggregation features without window
+ `service` Add support for updating feature store details

### 🐛 Bug Fixes

+ `service` Fix error when using request column as key in dictionary feature operations

## v1.1.1 (2024-06-10)

### 💡 Enhancements

+ `sdk-api` Use workspace home as default config path in databricks environment.

## v1.1.0 (2024-06-08)

### 🛑 Breaking Changes

+ `sdk-api` Skip filling null value by default for aggregated features.
+ `service` Rename FeatureJobSetting attributes to match the new naming convention.

### 💡 Enhancements

+ `service` Perform sampling operations without sorting tables
+ `service` Support offset parameter in aggregate_over and forward_aggregate
+ `service` Add default feature job settings to the SCDTable.
+ `dependencies` Bumped `freeware` to 0.2.18 to support new feature job settings
+ `service` Relax constraint that key has to be a lookup feature in dictionary operations
+ `dependencies` bump snowflake-connector-python

### 🐛 Bug Fixes

+ `service` Fix incorrect type casting in most frequent value UDF for Databricks Unity

## v1.0.3 (2024-05-21)

### 💡 Enhancements

+ `service` Backfill only required tiles for offline store tables when enabling a deployment
+ `service` Fix view and table describe method error on invalid datetime values
+ `service` Cast type for features with float dtype
+ `docker` Bump base docker image to python 3.10
+ `api` Introduce databricks accessor to deployment API object.
+ `api` Support specifying the target column when creating an observation table.
  + This change allows users to specify the target column when creating an observation table.
  + The target column is the column that contains the target values for the observations.
  + The target column name must match a valid target namespace name in the catalog.
  + The primary entities of the target namespace must match that of the observation table.
+ `service` Run feature computation queries in parallel
+ `service` Cast features with integer dtype BIGINT explicitly in feature queries
+ `api` Use async task for table / view / column describe to avoid timeout on large datasets.
+ `gh-actions` Migration to pytest-split to github actions
  + Databricks tests
  + Spark tests
+ `service` Avoid repeated graph flattening in GraphInterpreter and improve tile sql generation efficiency
+ `service` Skip casting data to string in describe query if not required
+ `sdk-api` Prevent users from creating a UDF feature that is not deployable.
+ `service` Run on demand tile computation concurrently
+ `service` Validate point in time and entity columns do not contain missing values in observation table
+ `service` Validate internal row index column is valid after features computation
+ `service` Improve precomputed lookup feature tables handling
+ `service` Support creating Target objects using forward_aggregate_asat
+ `service` Handle duplicate rows when looking up SCD and dimension tables
+ `service` Calculate entropy using absolute count values
+ `models` Limit asset names to 255 characters in length to ensure they can be referenced as identifiers in SQL queries
  + This change ensures that asset names are compatible with the maximum length of identifiers in SQL queries + This change will prevent errors when querying assets with long names
+ `dependencies` Bump dependencies to latest version
  1. snowflake-connector-python
  2. databricks-sdk
  3. databricks-sql-connector
+ `api` Add more associated objects to historical feature table objects.
+ `service` Create tile cache working tables in parallel

### ⚠️  Deprecations

+ `redis` Dropping aioredis as redis client library

### 🐛 Bug Fixes

+ `service` Fix offline store feature table name construction logic to avoid name collisions
+ `service` Fix ambiguous column name error when concatenating serving names
+ `service` Fix target SCD lookup code definition generation bug when the target name contains special characters.
+ `deps` Pinning pyopenssl to 24.X.X as client requirement
+ `service` Databricks integration is not working as expected.
+ `service` Fix KeyError caused by precomputed_lookup_feature_table_info due to backward compatibility issue
+ `session` Set active schema for the snowflake explicitly. The connector does not set the active schema specified.
+ `service` Fix an error when submitting data describe task payload
+ `session` Fix dtype detected wrongly for MAP type in Spark session
+ `api` Make dtype mandatory when create a target namespace
+ `session` Fix DataBricks relative frequency UDF to return None when all counts are 0
+ `service` Handle missing values in SCD effective timestamp and point in time columns
+ `session` Fix DataBricks entropy UDF to return 0 when all counts are 0
+ `udf` Fix division by zero in count dict cosine similarity UDFs
+ `dependencies` Bumping vulnerable dependencies
  + orjson
  + cryptography
  + ~~fastapi~~ (Need to bump to pydantic 2.X.X)
  + python-multipart
  + aiohttp
  + jupyterlab
  + black
  + pymongo
  + pillow
+ `session` Set ownership of created tables to the session group. This is a fix for the issue where the tables created cannot be updated by other users in the group.


## v1.0.2 (2024-03-15)

### 🐛 Bug Fixes

+ `service` Databricks integration fix

## v1.0.1 (2024-03-12)

### 💡 Enhancements

+ `api` Support description specification during table creation.
+ `api` Create api to manage online stores
+ `session` Specify role and group in Snowflake and Databricks details to enforce permissions for accessing source and output tables
+ `service` Simplify user defined function route creation schema
+ `online_serving` Implement FEAST offline stores for Spark Thrift and DataBricks for online serving support
+ `service` Compute data description in batches of columns
+ `service` Support offset parameter for aggregate_asat
+ `profile` Create a profile from databricks secrets to simplify access from a Databricks workspace.
+ `service` Improve efficiency of feature table cache checks for saved feature lists
+ `session` Add client_session_keep_alive to snowflake connector to keep the session alive
+ `service` Support cancellation for historical features table creation task

### 🐛 Bug Fixes

+ `service` Updates output variable type of count aggregation to be integer instead of float
+ `service` Fix FeatureList online_enabled_feature_ids attribute not updated correctly in some cases
+ `session` Fix snowflake session using wrong role if the user's default role does not match role in feature store details
+ `session` Fix count dictionary entropy UDF behavior for edge cases
+ `deployment` Fix getting sample entity serving names for deployment fails when entity has null values
+ `service` Fix ambiguous column name error when using SCD lookup features with different offsets

## v1.0.0 (2023-12-21)

### 💡 Enhancements

+ `session` Implement missing UDFs for DataBricks clusters that support Unity Catalog.
+ `storage` Support azure blob storage for file storage.

### 🐛 Bug Fixes

+ `service` Fixes a bug where the feature saving would fail if the feature or colum name contains quotes.
+ `deployment` Fix an issue where periodic tasks were not disabled when reverting a failed deployment

## v0.6.2 (2023-12-01)

### 🛑 Breaking Changes

+ `api` Support using observation tables in feature, target and featurelist preview
  + Parameter `observation_set` in `Feature.preview`, `Target.preview` and `FeatureList.preview` now accepts `ObservationTable` object or pandas dataframe
  + Breaking change: Parameter `observation_table` in `FeatureList.compute_historical_feature_table` is renamed to `observation_set`
+ `feature_list` Change feature list catalog output dataframe column name from `primary_entities` to `primary_entity`

### 💡 Enhancements

+ `databricks-unity` Add session for databricks unity cluster, and migrate one UDF to python for databricks unity cluster.
+ `target` Allow users to create observation table with just a target id, but no graph.
+ `service` Support latest aggregation for vector columns
+ `service` Update repeated columns validation logic to handle excluded columns.
+ `endpoints` Enable observation table to associate with multiple use cases from endpoints
+ `target` Derive window for lookup targets as well
+ `service` Add critical data info validation logic
+ `api` Implement remove observation table from context
+ `service` Support rename of context, use case, observation table and historical feature table
+ `target_table` Persist primary entity IDs for the target observation table
+ `observation_table` Update observation table creation check to make sure primary entity is set
+ `service` Implement service to materialize features to be published to external feature store
+ `service` Add feature definition hash to new feature model to allow duplicated features to be detected
+ `observation_table` Track uploaded file name when creating an observation table from an uploaded file.
+ `observation_table` Add way to update purpose for observation table.
+ `tests` Use published featurebyte library in notebook tests.
+ `service` Reduce complexity of describe query to avoid memory issue during query compilation
+ `session` Use DBFS for Databricks session storage to simplify setup
+ `target_namespace` Add support for target namespace deletion
+ `observation_table` add minimum interval between entities to observation table
+ `api` Implement delete observation table from use case
+ `api` Implement removal of default preview and eda table for context
+ `api` Enable observation table to associate with multiple use cases from api
+ `api` Implement removal of default preview and eda table for use case

### 🐛 Bug Fixes

+ `observation_table` fix validation around primary entity IDs when creating observation tables
+ `worker` Use cpu worker for feature job setting analysis to avoid blocking io worker async loop
+ `session` Make data warehouse session creation asynchronous with a timeout to avoid blocking the asyncio main thread. This prevents the API service from being unresponsive when certain compute clusters takes a long time to start up.
+ `service` Fix observation table sampling so that it is always uniform over the input
+ `worker` Fix feature job setting analysis fails for databricks feature store
+ `session` Fix spark session failing with spark version >= 3.4.1
+ `service` Fix observation table file upload error
+ `target` Support value_column=None for count in forward_aggregate/target operations.
+ `service` Fix division by zero error when calling describe on empty views
+ `worker` Fix bug where feature job setting analysis backtest fails when the analysis is missing an optional histogram
+ `service` Fixes a view join issue that causes the generated feature not savable due to graph inconsistency.
+ `use_case` Allow use cases to be created with descriptive only targets
+ `service` Fixes an error when rendering FeatureJobStatusResult in notebooks when matplotlib package is not available.
+ `feature` Fix feature saving bug when the feature contains timestamp filtering

## v0.6.1 (2023-11-22)

### 🐛 Bug Fixes

+ `api` fixed async task return code


## v0.6.0 (2023-10-10)

### 🛑 Breaking Changes

+ `observation_table` Validate that entities are present when creating an observation table.

### 💡 Enhancements

+ `target` Use window from target namespace instead of the target version.
+ `service` UseCase creation to accept TargetNameSpace id as a parameter
+ `historical_feature_table` Make FeatureClusters optional when creating historical feature table from UI.
+ `service` Move online serving code template generation to the online serving service
+ `model` Handle old Context records with entity_ids attribute in the database
+ `service` Add key_with_highest_value() and key_with_lowest_value() for cross aggregates
+ `api` Add consistent table feature job settings validation during feature creation.
+ `api` Change Context Entity attribute's name to Primary Entity
+ `api` Use primary entity parameter in Target and Context creation
+ `service` Add last_updated_at in FeatureModel to indicate when feature value is last updated
+ `api` Revise feature list create new version to avoid throwing error when the feature list is the same as the previous version
+ `service` Support rprefix parameter in View's join method
+ `observation_table` Add an optional purpose to observation table when creating a new observation table.
+ `docs` Documentation for Context and UseCase
+ `observation_table` Track earliest point in time, and unique entity col counts as part of metadata.
+ `service` Support extracting value counts and customised statistics in PreviewService
+ `api` Remove direct observation table reference from UseCase
+ `warehouse` improve data warehouse asset validation
+ `api` Use EntityBriefInfoList for entity info for both UseCase and Context
+ `api` Add trigo functions to series.
+ `api` Include observation table operation into Context API Object
+ `observation_table` Add route to allow users to upload CSV files to create observation tables.
+ `target` Tag entity_ids when creating an observation table from a target.
+ `api-client` improve api-client retry
+ `service` Entity Validation for Context, Target and UseCase
+ `service` Add Context Info method into both Context API Object and Route
+ `api` Add functionality to calculate haversine distance.
+ `service` Fix PreviewService describe() method when stats_names are provided

### 🐛 Bug Fixes

+ `service` Validate non-existent Target and Context when creating Use Case
+ `session` Fix execute query failing when variant columns contain null values
+ `service` Validate null target_id when adding obs table to use case
+ `service` Fix maximum recursion depth exceeded error in complex queries
+ `service` Fix race condition when accessing cached values in ApiObject's get_by_id()
+ `hive` fix hive connection error when spark_catalog is not the default
+ `api` Target#list should include items in target namespace.
+ `target` Fix target definition SDK code generation by skipping project.
+ `service` Fix join validation logic to account for rprefix


## v0.5.1 (2023-09-08)

### 💡 Enhancements

+ `service` Optimize feature readiness service update runtime.

### 🐛 Bug Fixes

+ `packaging` Restore cryptography package dependency [DEV-2233]

## v0.5.0 (2023-09-06)

### 🛑 Breaking Changes

+ `Configurations` Configurations::use_profile() function is now a method rather than a classmethod
  ```diff
  - Configurations.use_profile("profile")
  + Configurations().use_profile("profile")
  ```

### 💡 Enhancements

+ `service` Cache view created from query in Spark for better performance
+ `vector-aggregation` Add java UDAFs for sum and max for use in spark.
+ `vector-operations` Add cosine_similarity to compare two vector columns.
+ `vector-aggregation` Add integration test to test end to end for VECTOR_AGGREGATE_MAX.
+ `vector-aggregations` Enable vector aggregations for tiling aggregate - max and sum - functions
+ `middleware` Organize exceptions to reduce verbosity in middleware
+ `api` Add support for updating description of table columns in the python API
+ `vector-aggregation` Update groupby logic for non tile based aggregates
+ `api` Implement API object for Use Case component
+ `api` Use Context name instead of Context id for the API signature
+ `api` Implement API object for Context
+ `vector_aggregation` Add UDTF for max, sum and avg for snowflake.
+ `api` Integrate Context API object for UseCase
+ `vector-aggregation` Snowflake return values for vector aggregations should be a list now, instead of a string.
+ `vector-aggregation` Add java UDAFs for average for use in spark.
+ `vector_aggregation` Only return one row in table vector aggregate function per partition
+ `service` Support conditionally updating a feature using a mask derived from other feature(s)
+ `vector-aggregation` Add guardrails to prevent array aggregations if agg func is not max or avg.
+ `service` Tag semantics for all special columns during table creation
+ `api` Implement UseCase Info
+ `service` Change join type to inner when joining event and item tables
+ `vector-aggregation` Register vector aggregate max, and update parent dtype inference logic.
+ `service` Implement scheduled task to clean up stale versions and drop online store tables when possible
+ `use-case` Implement guardrail for use case's observation table not to be deleted
+ `vector-aggregations` Enable vector aggregations for tiling aggregate avg function
+ `api` Rename description update functions for versioned assets
+ `vector-aggregation` Support integer values in vectors; add support integration test for simple aggregates
+ `vector-aggregation` Update groupby_helper to take in parent_dtype.
+ `httpClient` added a ssl_verify value in Configurations to allow disabling of ssl certificate verification
+ `online-serving` Split online store compute and insert query to minimize table locking
+ `tests` Use the notebook as the test id in the notebook tests.
+ `vector-aggregation` Add simple average spark udaf.
+ `vector-aggregation` Add average snowflake udtf.
+ `api` Associate Deployment with UseCase
+ `service` Skip creating a data warehouse session when online disabling a feature
+ `use-case` implement use case model and its associated routes
+ `service` Apply event timestamp filter on EventTable directly in scheduled tile jobs when possible

### 🐛 Bug Fixes

+ `worker` Block running multiple concurrent deployment create/update tasks for the same deployment
+ `service` Fix bug where feature job starts running while the feature is still being enabled
+ `dependencies` upgrading `scipy` dependency
+ `service` Fixes an invalid identifier error in sql when feature involves a mix of filtered and non-filtered versions of the same view.
+ `worker` Fixes a bug where scheduler does not work with certain mongodb uris.
+ `online-serving` Fix incompatible column types when inserting to online store tables
+ `service` Fix feature saving error due to tile generation bug
+ `service` Ensure row ordering of online serving output DataFrame matches input request data
+ `dependencies` Limiting python range to 3.8>=,<3.12 due to scipy constraint
+ `service` Use execute_query_long_running when inserting to online store tables to fix timeout errors
+ `model` Mongodb index on periodic task name conflicts with scheduler engine
+ `service` Fix conversion of date type to double in spark


## v0.4.4 (2023-08-29)

### 🐛 Bug Fixes

+ `api` Fix logic for determining timezone offset column in datetime accessor
+ `service` Fix SDK code generation for conditional assignment when the assign value is a series
+ `service` Fix invalid identifier error for complex features with both item and window aggregates

### 💡 Enhancements

+ `profile` Allow creating of profile directly with fb.register_profile(name, url, token)


## v0.4.3 (2023-08-21)

### 🐛 Bug Fixes

+ `service` Fix feature materialization error due to ambiguous internal column names
+ `service` Fix error when generating info for features in some edge cases
+ `api` Fix item table default job settings not synchronized when job settings are updated in the event table, fix historical feature table listing failure

## v0.4.2 (2023-08-07)

### 🛑 Breaking Changes

+ `target` Update compute_target to return observation table instead of target table
  will make it easier to use with compute historical features
+ `target` Update target info to return a TableBriefInfoList instead of a custom struct
  this will help keep it consistent with feature, and also fix a bug in info where we wrongly assumed there was only one input table.

### 💡 Enhancements

+ `target` Add as_target to SDK, and add node to graph when it is called
+ `target` Add fill_value and skip_fill_na to forward_aggregate, and update name
+ `target` Create lookup target graph node
+ `service` Speed up operation structure extraction by caching the result of _extract() in BaseGraphExtractor

### 🐛 Bug Fixes

+ `api` Fix api objects listing failure in some notebooks environments
+ `utils` Fix is_notebook check to support Google Colab [https://github.com/featurebyte/featurebyte/issues/1598]

## v0.4.1 (2023-07-25)

### 🛑 Breaking Changes

+ `online-serving` Update online store table schema to use long table format
+ `dependencies` Limiting python version from >=3.8,<4.0 to >=3.8,<3.13 due to scipy version constraint

### 💡 Enhancements

+ `generic-function` add user-defined-function support
+ `target` add basic API object for Target
  Initialize the basic API object for Target.
+ `feature-group` update the feature group save operation to use `/feature/batch` route
+ `service` Update describe query to be compatible with Spark 3.2
+ `service` Ensure FeatureModel's derived attributes are derived from pruned graph
+ `target` add basic info for Target
  Adds some basic information about Target's. Additional information that contains more details about the actual data will be added in a follow-up.
+ `list_versions` update Feature's & FeatureList's `list_versions` method by adding `is_default` to the dataframe output
+ `service` Move TILE_JOB_MONITOR table from data warehouse to persistent
+ `service` Avoid using SHOW COLUMNS to support Spark 3.2
+ `table` skip calling data warehouse for table metadata during table construction
+ `target` add ForwardAggregate node to graph for ForwardAggregate
  Implement ForwardAggregator - only adds node to graph. Node is still a no-op.
+ `service` Add option to disable audit logging for internal documents
+ `query-graph` optimize query graph pruning computation by combining multiple pruning tasks into one
+ `target` add input data and metadata for targets
  Add more information about target metadata.
+ `target` Add primary_entity property to Target API object.
+ `service` Refactor FeatureManager and TileManager as services
+ `tests` Move tutorial notebooks into the FeatureByte repo
+ `service` Replace ONLINE_STORE_MAPPING data warehouse table by OnlineStoreComputeQueryService
+ `feature` block feature readiness & feature list status transition from DRAFT to DEPRECATED
+ `task_manager` refactor task manager to take celery object as a parameter, and refactor task executor to import tasks explicitly
+ `feature` fix bug with feature_list_ids not being updated after a feature list is deleted
+ `service` Replace TILE_FEATURE_MAPPING table in the data warehouse with mongo persistent
+ `target` perform SQL generation for forward aggregate node
+ `feature` fix primary entity identification bug for time aggregation over item aggregation features
+ `feature` limit manual default feature version selection to only the versions with highest readiness level
+ `feature-list` revise feature list saving to reduce api calls
+ `service` Refactor tile task to use dependency injection
+ `service` Fix error when disabling features created before OnlineStoreComputeQueryService is introduced
+ `deployment` Skip redundant updates of ONLINE_STORE_MAPPING table
+ `static-source-table` support materialization of static source table from source table or view
+ `catalog` Create target_table API object
  Remove default catalog, require explicit activation of catalog before catalog operations.
+ `feature-list` update feature list to preserve feature order
+ `target` Add gates to prevent target from setting item to non-target series.
+ `target` Add TargetNamespace#create
  This will allow us to register spec-like versions of a Target, that don't have a recipe attached.
+ `deployment` Reduce unnecessary backfill computation when deploying features
+ `service` Refactor TileScheduler as a service
+ `target` stub out target namespace schema and models
+ `service` Add traceback to tile job log for troubleshooting
+ `target` add end-to-end integration test for target, and include preview endpoint in target
+ `feature` update feature & feature list save operation to use POST `/feature/batch` route
+ `service` Disable tile monitoring by default
+ `service` Fix listing of databases and schemas in Spark 3.2
+ `target` Refactor compute_target and compute_historical_feature
+ `feature` optimize time to deserialize feature model
+ `entity-relationship` remove POST /relationship_info, POST /entity/parent and DELETE /entity/parent/<parent_entity_id> endpoints
+ `service` Support description update and retrieval for all saved objects
+ `config` Add default_profile in config to allow for a default profile to be set, and require a profile to be set if default_profile is not set
+ `target` Create target_table API object
  Create the TargetTable API object, and stub out the compute_target endpoint.
+ `target` Add datetime and string accessors into the Target API object.
+ `service` Fix unnecessary usage of SQL functions incompatible with Spark 3.2 (ILIKE and DATEADD)
+ `preview` Improve efficiency of feature and feature list preview by reducing unnecessary tile computation
+ `service` Fix DATEADD undefined function error in Spark 3.2 and re-enable tests
+ `service` Implement TileRegistryService to track tile information in mongo persistent
+ `spark-session` add kerberos authentication and webhdfs support for Spark session
+ `service` Fix compatibility of string contains operation with Spark 3.2
+ `target` add CRUD API endpoints for Target
  First portion of the work to include the Target API object.
+ `target` Fully implement compute_target to materialize a dataframe
+ `service` Refactor info service by splitting out logic to their respective services.
  Most of the info service logic was not being reused. It also feels cleaner for each service to be responsible for its own info logic. This way, dependencies are clearer. We also refactor service initialization such that we consistently use the dependency injection pattern.
+ `online-serving` Use INSERT operation to update online store tables to address concurrency issues
+ `target` create target namespace when we create a target
+ `service` Fix more datetime transform compatibility issues in Spark 3.2
+ `storage` Add support for using s3 as storage for featurebyte service
+ `target` Create target_table services, routes, models and schema
  This will help us support materializing target tables in the warehouse.

### ⚠️ Deprecations

+ `target` remove blind_spot from target models as it is not used

### 🐛 Bug Fixes

+ `worker` fixed cpu threading model
+ `service` Fix feature definition for isin() operation
+ `online-serving` Fix the job_schedule_ts_str parameter when updating online store tables in scheduled tile tasks
+ `gh-actions` Add missing build dependencies for kerberos support.
+ `feature_readiness` fix feature readiness bug due to readiness is treated as string when finding default feature ID
+ `transforms` Update get_relative_frequency to return 0 when there is no matching label
+ `service` Fix OnlineStoreComputeQuery prematurely deleted when still in use by other features
+ `data-warehouse` Fix metadata schema update for Spark and Databricks and bump working version
+ `service` Fix TABLESAMPLE syntax error in Spark for very small sample percentage
+ `feature` fix view join operation bug which causes improper query graph pruning
+ `service` Fix a bug in add_feature() where entity_id was incorrectly attached to the derived column

## v0.4.0 yanked (2023-07-25)

## v0.3.1 (2023-06-08)

### 🐛 Bug Fixes

+ `websocket` make websocket client more resilient connection lost
+ `websocket` fix client failure when starting secure websocket connection

## v0.3.0 (2023-06-05)

### 💡 Enhancements

+ `guardrails` add guardrail to make sure `*Table` creation does not contain shared column name in different parameters
+ `feature-list` add `default_feature_fraction` to feature list object
+ `datasource` check if database/schema exists when listing schemas/tables in a datasource
+ `error-handling` improve error handling and messaging for Docker exceptions
+ `feature-list` Refactor `compute_historical_features()` to use the materialized table workflow
+ `workflows` Update daily cron, dependencies and lint workflows to use code defined github workflows.
+ `feature` refactor feature object to remove unused entity_identifiers, protected_columns & inherited_columns properties
+ `scheduler` implement soft time limit for io tasks using gevent celery worker pool
+ `list_versions()` add `is_default` column to feature's & feature list's `list_versions` object method output DataFrame
+ `feature` refactor feature class to drop `FrozenFeatureModel` inheritance
+ `storage` support GCS storage for Spark and DataBricks sessions
+ `variables` expose `catalog_id` property in the Entity and Relationship API objects
+ `historical-features` Compute historical features in batches of columns
+ `view-object` add `column_cleaning_operations` to view object
+ `logging` support overriding default log level using environment variable `LOG_LEVEL`
+ `list_versions()` remove `feature_list_namespace_id` and `num_feature` from `feature_list.list_versions()`
+ `feature-api-route` remove `entity_ids` from feature creation route payload
+ `historical-features` Improve tile cache performance by reducing unnecessary recalculation of tiles for historical requests
+ `worker` support `scheduler`, `worker:io`, `worker:cpu` in startup command to start different services
+ `feature-list` add `default_feature_list_id` to `feature_list.info()` output
+ `feature` remove `feature_namespace_id` (`feature_list_namespace_id`) from feature (feature list) creation payload
+ `docs` automatically create `debug` folder if it doesn't exist when running docs
+ `feature-list` add `primary_entities` to feature list's `list()` method output DataFrame
+ `feature` add POST `/feature/batch` endpoint to support batch feature creation
+ `table-column` add `cleaning_operations` to table column object & view column object
+ `workflows` Update workflows to use code defined github workflows.
+ `feature-session` Support Azure blob storage for Spark and DataBricks sessions
+ `feature` update feature's & feature list's version format from dictionary to string
+ `feature-list` refactor feature list class to drop `FrozenFeatureListModel` inheritance
+ `display` implement HTML representation for API objects `.info()` result
+ `feature` remove `dtype` from feature creation route payload
+ `aggregate-asat` Support cross aggregation option for aggregate_asat.
+ `databricks` support streamed records fetching for DataBricks session
+ `feature-definition` update feature definition by explicitly specifying `on` parameter in `join` operation
+ `source-table-listing` Exclude tables with names that has a "__" prefix in source table listing

### ⚠️ Deprecations

+ `middleware` removed TelemetryMiddleware
+ `feature-definition` remove unused statement from `feature.definition`
+ `FeatureJobSettingAnalysis` remove `analysis_parameters` from `FeatureJobSettingAnalysis.info()` result

### 🐛 Bug Fixes

+ `relationship` fixed bug that was causing an error when retrieving a `Relationship` with no `updated_by` set
+ `dependencies` updated `requests` package due to vuln
+ `mongodb` mongodb logs to be shipped to stderr to reduce disk usage
+ `deployment` fix multiple deployments sharing the same feature list bug
+ `dependencies` updated `pymdown-extensions` due to vuln `CVE-2023-32309`
+ `dependencies` fixed vulnerability in starlette
+ `api-client` API client should not handle 30x redirects as these can result in unexpected behavior
+ `mongodb` update `get_persistent()` by removing global persistent object (which is not thread safe)
+ `feature-definition` fixed bug in `feature.definition` so that it is consistent with the underlying query graph

## v0.2.2 (2023-05-10)

### 💡 Enhancements

+ Update healthcare demo dataset to include timezone columns

### 🐛 Bug Fixes

+ Drop a materialized table only if it exists when cleaning up on error
+ Added `dependencies` workflow to repo to check for dependency changes in PRs
+ Fixed taskfile `java` tasks to properly cache the downloaded jar files.

## v0.2.1 (2023-05-10)

### 🐛 Bug Fixes

+ Removed additional dependencies specified in featurebyte client


## v0.2.0 (2023-05-08)

### 🛑 Breaking changes

+ `featurebyte` is now available for early access
