# Changelog

---

## v0.2.3 (Unreleased)

### 🛑 Breaking changes 🛑
### 🚩 Deprecations 🚩
### 💡 Enhancements 💡

+ Support streamed records fetching for DataBricks session
+ Support GCS storage for Spark and DataBricks sessions
+ Update feature definition by explicitly specifying `on` parameter in `join` operation
+ Expose `catalog_id` property in the Entity and Relationship API objects
+ Remove unused statement from `feature.definition`
+ Improve error handling and messaging for Docker exceptions
+ Support `scheduler`, `worker:io`, `worker:cpu` in startup command to start different services
+ Update feature's & feature list's version format from dictionary to string
+ Implement HTML representation for API objects `.info()` result
+ Add `column_cleaning_operations` to view object
+ Exclude tables with names that has a "__" prefix in source table listing
+ Remove `analysis_parameters` from `FeatureJobSettingAnalysis.info()` result
+ Add `is_default` column to feature's & feature list's `list_versions` object method output DataFrame
+ Add guardrail to make sure `SCDTable`'s `effective_timestamp_column` differs from `end_timestamp_column`
+ Add `primary_entities` to feature list's `list()` method output DataFrame
+ Add `cleaning_operations` to table column object & view column object

### 🧰 Bug fixes 🧰

+ Fixed bug in `feature.definition` so that it is consistent with the underlying query graph
+ Updated `pymdown-extensions` due to vuln `CVE-2023-32309`
+ Fixed bug that was causing an error when retrieving a `Relationship` with no `updated_by` set

## v0.2.2

### 💡 Enhancements 💡

+ Update healthcare demo dataset to include timezone columns

### 🧰 Bug fixes 🧰

+ Drop a materialized table only if it exists when cleaning up on error
+ Added `dependencies` workflow to repo to check for dependency changes in PRs
+ Fixed taskfile `java` tasks to properly cache the downloaded jar files.

## v0.2.1

### 🧰 Bug fixes 🧰

* Removed additional dependencies specified in featurebyte client


## v0.2.0

### 🛑 Breaking changes 🛑

+ `featurebyte` is now available for early access
