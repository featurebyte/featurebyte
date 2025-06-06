SELECT
  COUNT(*)
FROM "cat1_cust_id_30m";

CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM (
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
  WHERE
    "event_timestamp" >= CAST(FLOOR(
      (
        EXTRACT(epoch_second FROM CAST(CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP)) - 300
      ) / 1800
    ) * 1800 + 300 - 600 - 1800 AS TIMESTAMP)
    AND "event_timestamp" < CAST(FLOOR(
      (
        EXTRACT(epoch_second FROM CAST(CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP)) - 300
      ) / 1800
    ) * 1800 + 300 - 600 AS TIMESTAMP)
)
WHERE
  NOT "cust_id" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ;
