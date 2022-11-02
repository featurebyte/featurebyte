# Feature Store

A feature store is a database residing on a data warehouse platform that is created specifically for managing feature metadata.

Since compute is performed in the warehouse, tables required for feature engineering must be accessible to the user account.

User account used for a feature store must be granted the following privileges

| Privilege       | Scope                                   | Purpose                                         |
|-----------------|-----------------------------------------|-------------------------------------------------|
| SELECT          | All tables used for feature engineering | Read data for feature computation               |
| INSERT / UPDATE | Feature Store database                  | Write feature and tile metadata                 |
| CREATE FUNCTION | Feature Store database                  | Create custom functions for feature engineering |

Multiple feature stores can be managed using the FeatureByte service.
!!! note "Multiple Feature Stores limitations"
    - Generating historical features only work for feature list using a single store currently.
    - Features cannot be computed from join tables between stores.
