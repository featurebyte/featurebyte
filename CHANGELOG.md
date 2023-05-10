# Changelog

---

## v0.2.2 (Unreleased)

---

### 🛑 Breaking changes 🛑


### 🚩 Deprecations 🚩


### 💡 Enhancements 💡

* Support using DataFrame as observation set in `compute_historical_feature_table()`
* Support `preview`, `sample` and `describe` methods for materialised tables
* Support `columns` and `columns_rename_mapping` parameters when creating observation table and
  batch feature table
* Update healthcare demo dataset to include timezone columns

### 🧰 Bug fixes 🧰

+ Drop a materialized table only if it exists when cleaning up on error

## v0.2.1

### 🧰 Bug fixes 🧰

* Removed additional dependencies specified in featurebyte client

---

## v0.2.0

---
### 🛑 Breaking changes 🛑

+ `featurebyte` is now available for early access
