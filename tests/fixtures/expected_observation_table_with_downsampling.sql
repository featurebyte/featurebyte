SELECT WORKING_SCHEMA_VERSION, FEATURE_STORE_ID FROM "sf_database"."sf_schema"."METADATA_SCHEMA";

UPDATE "sf_database"."sf_schema"."METADATA_SCHEMA" SET FEATURE_STORE_ID = '646f6c190ed28a5271fb02a1' WHERE 1=1;

SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table";

SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table";

CREATE TABLE "sf_database"."sf_schema"."__TEMP_OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  "cust_id",
  "POINT_IN_TIME"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME"
  FROM (
    SELECT
      *
    FROM "sf_database"."sf_schema"."sf_table"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
      "POINT_IN_TIME" IS NOT NULL AND
      "cust_id" IS NOT NULL;

CREATE TABLE "sf_database"."sf_schema"."missing_data_OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  "cust_id",
  "POINT_IN_TIME"
FROM (
  SELECT
    "cust_id" AS "cust_id",
    "POINT_IN_TIME" AS "POINT_IN_TIME"
  FROM (
    SELECT
      *
    FROM "sf_database"."sf_schema"."sf_table"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL OR
        "cust_id" IS NULL
    );

CREATE TABLE "REQUEST_TABLE_68DFBD342ECAABD7D21D4759" AS
SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX"
FROM "sf_database"."sf_schema"."__TEMP_OBSERVATION_TABLE_000000000000000000000000";

SELECT
  MIN("POINT_IN_TIME") AS "min",
  MAX("POINT_IN_TIME") AS "max"
FROM "REQUEST_TABLE_68DFBD342ECAABD7D21D4759";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH "REQUEST_TABLE_POINT_IN_TIME_cust_id" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "cust_id"
  FROM REQUEST_TABLE_68DFBD342ECAABD7D21D4759
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."cust_id",
    "T0"."_fb_internal_cust_id_forward_sum_col_float_cust_id_None_project_1" AS "_fb_internal_cust_id_forward_sum_col_float_cust_id_None_project_1"
  FROM REQUEST_TABLE_68DFBD342ECAABD7D21D4759 AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."cust_id" AS "cust_id",
      SUM(SOURCE_TABLE."col_float") AS "_fb_internal_cust_id_forward_sum_col_float_cust_id_None_project_1"
    FROM "REQUEST_TABLE_POINT_IN_TIME_cust_id" AS REQ
    INNER JOIN (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
    ) AS SOURCE_TABLE
      ON (
        DATE_PART(EPOCH_SECOND, SOURCE_TABLE."event_timestamp") > DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME")
        AND DATE_PART(EPOCH_SECOND, SOURCE_TABLE."event_timestamp") <= DATE_PART(EPOCH_SECOND, REQ."POINT_IN_TIME") + 86400
      )
      AND REQ."cust_id" = SOURCE_TABLE."cust_id"
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  (
    CASE
      WHEN (
        "_fb_internal_cust_id_forward_sum_col_float_cust_id_None_project_1" IS NULL
      )
      THEN 0.0
      ELSE "_fb_internal_cust_id_forward_sum_col_float_cust_id_None_project_1"
    END > 0
  ) AS "bool_target"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000" AS
SELECT
  REQ."POINT_IN_TIME",
  REQ."cust_id",
  T0."bool_target"
FROM "REQUEST_TABLE_68DFBD342ECAABD7D21D4759" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

DROP TABLE IF EXISTS "sf_database"."sf_schema"."__TEMP_000000000000000000000000_0";

DROP TABLE IF EXISTS "sf_database"."sf_schema"."REQUEST_TABLE_68DFBD342ECAABD7D21D4759";

DROP TABLE IF EXISTS "sf_database"."sf_schema"."__TEMP_OBSERVATION_TABLE_000000000000000000000000";

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
          *
        FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000"
      )
    )
    WHERE
        "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
          "POINT_IN_TIME" IS NOT NULL AND
          "cust_id" IS NOT NULL
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
      *
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000000"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL OR
        "cust_id" IS NULL
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
          *
        FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
      )
    )
    WHERE
        "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
          "POINT_IN_TIME" IS NOT NULL AND
          "cust_id" IS NOT NULL
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
      *
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL OR
        "cust_id" IS NULL
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
      *
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
      "POINT_IN_TIME" IS NOT NULL AND
      "cust_id" IS NOT NULL;

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
      *
    FROM "sf_database"."sf_schema"."OBSERVATION_TABLE_000000000000000000000001"
  )
)
WHERE
    "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
    (
        "POINT_IN_TIME" IS NULL OR
        "cust_id" IS NULL
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
