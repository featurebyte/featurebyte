# Changelog

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
