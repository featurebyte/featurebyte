# Changelog

---

## v0.1.0 [Unreleased]

---

### 🛑 Breaking changes 🛑


### 🚩 Deprecations 🚩

+ `databricks-sql-connector`: removed in favour of `featurebyte-databricks-sql-connector`

### 💡 Enhancements 💡

+ `changelog`: Added changelog checker github workflow
+ `changelog`: Added support for data describe in API
+ `changelog`: Added support for timestamps with timezone offsets
+ `changelog`: Include serving endpoint information in FeatureList.info()
+ `changelog`: Added FeatureJobSettingAnalysis api to list and retrieve job setting analysis
+ `changelog`: Added support to download feature job setting analysis report as pdf
+ `changelog`: Added feature / featurelist get_feature_jobs_status() and list_features() for featurelist
+ `changelog`: Added workspaces

### 🧰 Bug fixes 🧰

+ `changelog`: Fix error in item view describe()
+ `changelog`: Fix infinite values showing up as nan in data / view describe
+ `changelog`: Fix automated feature job settings update failure when timestamps contain timezone

## v0.0.0

---
