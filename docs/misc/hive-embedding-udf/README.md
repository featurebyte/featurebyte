# Example of Hive UDF for embeddings via Transformer-based models

This project contains an example implementation of Hive UDF which calls externally deployed embedding (transformer) model.
This UDF can be used with Databricks and as well as other environments based on Apache Spark.

> **Note**: The goal of this project is to show an example of how Hive Embedding UDF can be implemented, not to provide production ready solution.
> It doesn't cover all possible model serving approaches and authentication schemes.

### Prerequisites
1. [Java 11+](https://docs.oracle.com/en/java/javase/11/install/overview-jdk-installation.html)
2. [Gradle Build Tool](https://gradle.org/install/)

### How-To

The project assumes model to be awailable over http.
Right now following serving approaches are supported:
- GCP Cloud functions
- Databricks MLFlow (vanilla MLFlow will likely also work with some adjustments)
- Custom TorchServe service deployment

##### Steps

1. Build jar file
    ```bash
    ./gradlew clean shadowJar
    ```
    The jar file will be saved under `lib/build/libs` directory.
2. Add the jar file to Spark. For more details refer to [official Databricks documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-udf-hive.html) or [Spark documentation](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-resource-mgmt-add-jar.html)
3. Create and test SQL function:
    ```sql
    -- add jar file, refer to step 2.
    ADD JAR '/path/to/jar/featurebyte-hive-embedding-udf-all.jar';

    -- create SQL function using the jar file
    CREATE OR REPLACE FUNCTION F_EMBEDDING AS 'com.featurebyte.hive.embedding.udf.EmbeddingUDF'
    USING JAR '/path/to/jar/featurebyte-hive-embedding-udf-all.jar';

    -- simple test for SQL function
    select
        sentence,
        F_EMBEDDING(sentence) as vector
    from (
        select 'This is first example' as sentence
        union
        select 'This is sencond example' as sentence
    );
    ```
