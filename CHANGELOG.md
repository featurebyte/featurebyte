# Changelog

## v0.2.2

### 💡 Enhancements

+ Update healthcare demo dataset to include timezone columns

### 🐛 Bug Fixes

+ Drop a materialized table only if it exists when cleaning up on error
+ Added `dependencies` workflow to repo to check for dependency changes in PRs
+ Fixed taskfile `java` tasks to properly cache the downloaded jar files.

## v0.2.1

### 🐛 Bug Fixes

* Removed additional dependencies specified in featurebyte client


## v0.2.0

### 🛑 Breaking changes

+ `featurebyte` is now available for early access
