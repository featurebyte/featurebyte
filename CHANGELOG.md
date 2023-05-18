# Changelog

---

## v0.2.3 (Unreleased)

### 🛑 Breaking changes 🛑
### 🚩 Deprecations 🚩
### 💡 Enhancements 💡
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
