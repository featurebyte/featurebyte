SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM "sf_database"."sf_schema"."METADATA_SCHEMA";

UPDATE "sf_database"."sf_schema"."METADATA_SCHEMA" SET FEATURE_STORE_ID = '646f6c190ed28a5271fb02a1' WHERE 1=1;

SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_with_target_namespace";

SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_with_target_namespace";

SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_with_target_namespace";

CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "bool_target"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "bool_target" AS "bool_target"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "bool_target" AS "bool_target"
    FROM "sf_database"."sf_schema"."sf_table_with_target_namespace"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
      "POINT_IN_TIME" IS NOT NULL AND
      "bool_target" IS NOT NULL;

CREATE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "bool_target"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "bool_target" AS "bool_target"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "bool_target" AS "bool_target"
    FROM "sf_database"."sf_schema"."sf_table_with_target_namespace"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL OR
        "bool_target" IS NULL
    );

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "OBSERVATION_TABLE_000000000000000000000000";

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "missing_data_OBSERVATION_TABLE_000000000000000000000000";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000";

CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target",
  CASE WHEN "bool_target" = TRUE THEN CAST(2.0 AS FLOAT) ELSE 5.0 END AS "__FB_TABLE_ROW_WEIGHT"
FROM (
  SELECT
    CAST(BITAND(RANDOM(0), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "cust_id",
    "POINT_IN_TIME",
    "float_target",
    "bool_target"
  FROM (
    SELECT
      "cust_id",
      "POINT_IN_TIME",
      "float_target",
      "bool_target"
    FROM (
      SELECT
        "cust_id" AS "cust_id",
        "POINT_IN_TIME" AS "POINT_IN_TIME",
        "float_target" AS "float_target",
        "bool_target" AS "bool_target"
      FROM (
        SELECT
          "cust_id" AS "cust_id",
          "POINT_IN_TIME" AS "POINT_IN_TIME",
          "float_target" AS "float_target",
          "bool_target" AS "bool_target"
        FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000"
      )
    )
    WHERE
        "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
          "POINT_IN_TIME" IS NOT NULL
  )
)
WHERE
  (
    "bool_target" = TRUE AND "prob" <= 0.5
  )
  OR (
    NOT "bool_target" IN (TRUE) AND "prob" <= 0.2
  );

CREATE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000001" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "float_target" AS "float_target",
    "bool_target" AS "bool_target"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "float_target" AS "float_target",
      "bool_target" AS "bool_target"
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL
    );

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "OBSERVATION_TABLE_000000000000000000000001";

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000001" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "missing_data_OBSERVATION_TABLE_000000000000000000000001";

SELECT
  DISTINCT "bool_target"
FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000002" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target",
  CASE
    WHEN "bool_target" = TRUE
    THEN CAST("__FB_TABLE_ROW_WEIGHT" * 3.3333333333333335 AS FLOAT)
    ELSE "__FB_TABLE_ROW_WEIGHT"
  END AS "__FB_TABLE_ROW_WEIGHT"
FROM (
  SELECT
    CAST(BITAND(RANDOM(0), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "cust_id",
    "POINT_IN_TIME",
    "float_target",
    "bool_target",
    "__FB_TABLE_ROW_WEIGHT"
  FROM (
    SELECT
      "cust_id",
      "POINT_IN_TIME",
      "float_target",
      "bool_target",
      "__FB_TABLE_ROW_WEIGHT"
    FROM (
      SELECT
        "cust_id" AS "cust_id",
        "POINT_IN_TIME" AS "POINT_IN_TIME",
        "float_target" AS "float_target",
        "bool_target" AS "bool_target",
        "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
      FROM (
        SELECT
          "cust_id" AS "cust_id",
          "POINT_IN_TIME" AS "POINT_IN_TIME",
          "float_target" AS "float_target",
          "bool_target" AS "bool_target",
          "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
        FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
      )
    )
    WHERE
        "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
          "POINT_IN_TIME" IS NOT NULL
  )
)
WHERE
  (
    "bool_target" = TRUE AND "prob" <= 0.3
  )
  OR (
    NOT "bool_target" IN (TRUE) AND "prob" <= 1.0
  );

CREATE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000002" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target",
  "__FB_TABLE_ROW_WEIGHT"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "float_target" AS "float_target",
    "bool_target" AS "bool_target",
    "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "float_target" AS "float_target",
      "bool_target" AS "bool_target",
      "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL
    );

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000002" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "OBSERVATION_TABLE_000000000000000000000002";

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000002" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "missing_data_OBSERVATION_TABLE_000000000000000000000002";

SELECT
  DISTINCT "bool_target"
FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000002";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

SHOW COLUMNS IN "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001";

CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000003" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target",
  "__FB_TABLE_ROW_WEIGHT"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "float_target" AS "float_target",
    "bool_target" AS "bool_target",
    "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "float_target" AS "float_target",
      "bool_target" AS "bool_target",
      "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
      "POINT_IN_TIME" IS NOT NULL;

CREATE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000003" AS
SELECT
  "cust_id",
  "POINT_IN_TIME",
  "float_target",
  "bool_target",
  "__FB_TABLE_ROW_WEIGHT"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "float_target" AS "float_target",
    "bool_target" AS "bool_target",
    "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
  FROM (
    SELECT
      "cust_id" AS "cust_id",
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "float_target" AS "float_target",
      "bool_target" AS "bool_target",
      "__FB_TABLE_ROW_WEIGHT" AS "__FB_TABLE_ROW_WEIGHT"
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL
    );

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000003" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "OBSERVATION_TABLE_000000000000000000000003";

CREATE OR REPLACE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000003" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "missing_data_OBSERVATION_TABLE_000000000000000000000003";

SELECT
  DISTINCT "bool_target"
FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000003";
