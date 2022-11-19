# What is FeatureByte

FeatureByte is a free and open access platform that **radically simplifies the creation and serving of features needed for machine learning**.

FeatureByte makes it easy to transform data, declare features, and run experiments in a notebook. After successful experiments, a feature list can be deployed without creating separate pipelines or using different tools. The deployment complexity is abstracted away from users and the orchestration of the feature materialization into an online feature store is automatically triggered. With no effort, low latency feature serving is available for model inference via a REST API service.

### Rich palette of feature signals for creative Data Scientists
Thanks to features extracted with FeatureByte, machine learning models can benefit from data scientists’ creativity and a rich palette of signals that range from:

* features from Slowly Changing Data, such as whether the customer changed address in the past 4 weeks and the distance between current and previous residence locations,
* to window aggregates and feature transforms from business Event Data such as the sum spent by a customer the past 14 days, and the clumpiness of her orders over the past 12 weeks
* or to more complex diversity, stability and similarity measures of a customer’s shopping basket from Item Data.

### Intuitive Feature Engineering
The user declaration of features is facilitated by a flexible Python SDK, where data transformations (joins, subsets, conditional statements, lags and new columns) are declared in a similar way to Pandas.

### Data Leakage
Data leakage is prevented thanks to carefully designed and implemented built-in point-in-time joins, window aggregates, and feature transforms that extract features in a time-aware manner.

### Garbage in / Garbage out
As a preventative measure, data quality annotation at the data level is supported and strongly encouraged as well as the setting of default cleaning steps that are automatically applied if no cleaning steps are specified during feature declaration.

### No signal left behind
Relationships between business entities are automatically modeled to recommend features from parent entities, joins are recommended, and state-of-the-art features are suggested based on data semantics and codification of best practices developed by the community at FeatureByte.

### Feature Exploration and Experiment
Use cases define the level of analysis, the target and the context of the experiments. Thanks to the use case formulation, observation sets specifically designed for the use case are easily derived for EDA, training, retraining or test purposes.

### Target leakage
Targets are declared in the similar way as features but using forward joins and aggregates. At training time, targets are materialized together with features with the same points-in-time.

When a use case has a target with a long horizon such as customer churn the next 6 months, training observation sets are built such that the time interval between 2 observations of the same customer is always larger than the target horizon to avoid target leakage.

### Feature Leakage
For use cases related to an Event Entity, features are served using windows that exclude the event of the request. For example, in a transaction fraud detection, FeatureByte’s windowed aggregation implementation ensures the feature windows exclude the current transaction and avoids leaks when comparing the current transaction to previous transactions.

When a feature is derived from 2 features, the computation of the 2 features is scheduled at the same time to ensure windows of the 2 features are consistent.

### Collaboration
To allow collaboration with external contributors and facilitate prototyping, csv or parquet snapshots can be used to run modeling experiments, such as feature list tuning.

Use cases and their associated data models can also be shared in FeatureByte’s community platform to collect semantics and feature suggestions from the community or the user’s private network.

### Training-Serving Inconsistencies
The FeatureByte scheduling engine automatically analyzes source data availability and freshness and recommends a default setting for Feature Job scheduling, abstracting the complexity of setting Feature Jobs.

### Feature Versioning

When new data quality issues arise, or there are changes in the management of the source data, new feature versions can be triggered without disrupting the serving of feature lists already deployed.

Each feature version has a feature lineage. It is then possible to audit features before deploying them, and to derive similar features in the future.

### Security and resource optimization
To minimize security risks, leverage scalability of cloud data platform and reduce resource usage and storage, FeatureByte follows core principles in the solution implementation:

* Use data sources that reside in scalable cloud data warehouses such as Snowflake and Databricks
* Don’t move data. Computation on data is performed only in the data warehouse, leveraging the scalability, stability and efficiency they provide. There is also no bulk outbound data transfer which minimizes security risks.
* Use a tiling approach to pre-compute features whenever possible

### Feature Lifecycle management
To fully empower team of data scientists, FeatureByte is providing enterprises with capabilities to manage the complete lifecycle of features under its commercial license in the coming months:

* Enterprise-grade Security
* Role Based Access Control and data privacy
* Self Organized Feature Catalog
* CI/CD pipeline for Views and Features
* Feature List Deployment Governance
* Project collaboration platform
* Source data observability
* Feature observability
* Feature redundancy analysis
* Training-Serving consistency monitoring
* Feature usage tracking
* Resource usage
* Use Case Accuracy monitoring


## Get Started!
Check out the [Installation](../get_started/installation.md) section for instructions to get up and running and the [Quickstart](../get_started/quickstart.md) to get up to speed!
