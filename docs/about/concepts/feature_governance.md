When new data quality issues arise, or there are changes in the management of the source data, new feature versions can be triggered without disrupting the serving of feature lists already deployed.

Each feature version has a feature lineage. It is then possible to audit features before deploying them, and to derive similar features in the future.

Status is associated with Features and Feature Lists to help inform on their readiness.

### Feature Versioning
Feature versioning is supported to handle undesirable changes in the management or the data quality of the data sources.

In the case of changes in the management of the data sources, a new default for the Feature Job Setting can be set and new versions of the feature can be created with a new FeatureJob Setting.

In the case of changes in the data quality of the data sources, new default cleaning steps can be annotated to the data columns affected by the change and the creation of new feature versions is facilitated for features using those columns as input.

Old versions of the feature can be still served to not disrupt the inference of Machine Learning tasks that rely on the feature.

#### Changes in Data Quality annotation
If a column has not been used by a feature yet, the data quality information associated with this column can be easily updated.

Once the column has been used to create a feature, users are required to create a plan and submit this plan for approval. The plan is created from a data source by specifying the desired updates.

From the plan, users can list Feature versions with inappropriate cleaning steps setting and Feature List versions that include affected feature versions.

By default, the creation of a new feature version is recommended. The user can however choose to overwrite the current version. To help users evaluate the impact of the changes, users can materialize the features before and after change by selecting an observation set.

Once the new cleaning steps settings are defined for every affected feature version, the plan can be submitted. Once the plan is approved, it is applied to the data source and new feature versions are created accordingly.

If the option of a new feature version creation is chosen, the new version inherits from the current readiness of the older version and the old version is automatically deprecated. If the old version is the default version of the feature, the new version becomes the default version.

### Feature List versioning
FeatureByte supports Feature List Versioning. 3 modes can be used:

* “auto”: a new version of the Feature List is triggered by changes in version of the features in the list. The new default version of the Feature List then uses the current default versions of its features.
* “manual”: new versions are created manually. Only the versions of the features that are specified are changed. The versions of other features are the same as the origin Feature List version.
* “semi-auto”: The default version of the Feature List uses the current default versions of features except for the features versions that are specified.

The “auto” mode is the default mode.

### Feature Lineage
Each feature version has a feature lineage. It is then possible to audit features before deploying them, and to derive similar features in the future.

The SDK code used to declare the feature and the SQL code used to execute its computation are both provided.

To improve the readability of the SDK code, code is pruned to present only steps related to the feature and is automatically organized in the form of key steps (joins, columns derivations, aggregation and post aggregation transforms).

### Feature Readiness
4 Feature Readiness levels are recognized by FeatureByte:

* Production_ready: “Ready to be served in production”
* Draft: “Shared for training purposes only”
* Quarantine: “Had recently some issues. To use with caution. Under review.”
* Deprecated: “Not recommended neither for training nor for online serving”

The Quarantine level is triggered automatically if issues are raised to remind users of the need for actions: fix warehouse jobs, fix quality issues or create new feature versions to retrain and serve Machine Learning models with healthier versions.

When Users are calling for a feature without specifying its version, the default version is returned. The default version of a Feature is the version with the highest readiness unless changed by the user.

### Feature List status
FeatureByte recognizes 5 status for a Feature List:

* “Deployed”: are Feature Lists that have at least one version enabled online
* “Template”: are Feature Lists that are created to be used as references for users willing to build new Feature Lists although it is not a deployed Feature List. It constitutes for them a safe starting point.
* “Public Draft”: are Feature Lists that Data Scientists want to share with others to get feedback.
* “Draft”: are Feature Lists that can be accessed only by their authors. They are very unlikely to be used as a final product. They are usually produced by data scientists who are running experiments for a use case.
* “Deprecated”: are old Feature Lists that are not useful or recommended any more.

Before a Feature List is turned into a Template, a description should be associated with the Feature List and all features readiness should be turned into “production ready”.

The Deployed status is automatically triggered when at least one version of the FeatureList is deployed. If deployment is disabled for all the Feature List versions, the status of the Feature List is turned into a Public Draft.

Only Draft Feature Lists can be deleted. Feature Lists with other status can be only deprecated.
