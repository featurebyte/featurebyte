# Changelog

## v0.3.1

### 🐛 Bug Fixes

+ `websocket` make websocket client more resilient connection lost
+ `websocket` fix client failure when starting secure websocket connection

## v0.3.0

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

## v0.2.2

### 💡 Enhancements

+ Update healthcare demo dataset to include timezone columns

### 🐛 Bug Fixes

+ Drop a materialized table only if it exists when cleaning up on error
+ Added `dependencies` workflow to repo to check for dependency changes in PRs
+ Fixed taskfile `java` tasks to properly cache the downloaded jar files.

## v0.2.1

### 🐛 Bug Fixes

+ Removed additional dependencies specified in featurebyte client


## v0.2.0

### 🛑 Breaking changes

+ `featurebyte` is now available for early access
