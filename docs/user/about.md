# What is FeatureByte

FeatureByte is a suite of tools that makes it easy to define, deploy, share and maintain features for Machine Learning applications, using data sources that reside in scalable cloud data warehouses such as Snowflake and Databricks.

## Key Features

### Declarative Feature Creation
Features are defined as a graphical representation of intended operations.
The execution graph can be translated into platform-specific SQL (e.g. SnowSQL or SparkSQL), making it easier to adopt new technologies in the future.

### Push-down Computation
Computation on data is performed only in the data warehouse, leveraging the scalability, stability and efficiency they provide. There is also no bulk outbound data transfer which minimizes security risks.

### Intuitive Feature Engineering
Create features using a pandas-like SDK purpose-built for feature engineering.
Data assets including data sources, features and feature lists are designed to capture business logic to facilitate smarter feature engineering and feature reuse.

### Hassle-free deployment
Generate historical features for training, batch scoring and online serving without creating different pipelines or using different tools.
Avoid train-test skew using pre-defined feature job settings that apply across the board.


## Get Started!
Check out the [Installation](installation.md) section for instructions to get up and running and the [Quickstart](quickstart.md) to get up to speed!
