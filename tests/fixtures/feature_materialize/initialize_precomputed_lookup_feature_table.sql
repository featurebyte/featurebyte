SELECT
  COUNT(*)
FROM "cat1_gender_1d";

SELECT
  MAX("__feature_timestamp") AS RESULT
FROM "cat1_gender_1d";

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  "__feature_timestamp",
  "gender",
  "__feature_requiring_parent_serving_V220101__part1",
  "__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM (
  SELECT
    "__feature_timestamp",
    "gender",
    "__feature_requiring_parent_serving_V220101__part1",
    "__feature_requiring_parent_serving_plus_123_V220101__part1",
    ROW_NUMBER() OVER (PARTITION BY "gender" ORDER BY "__feature_timestamp" DESC NULLS LAST) AS "_row_number"
  FROM "cat1_gender_1d"
)
WHERE
  "_row_number" = 1;

CREATE TABLE "sf_db"."sf_schema"."TEMP_LOOKUP_UNIVERSE_TABLE_000000000000000000000000" AS
WITH ENTITY_UNIVERSE AS (
  SELECT
    CAST('2022-01-05 00:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    "cust_id"
  FROM (
    SELECT DISTINCT
      "col_text" AS "cust_id"
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "is_active" AS "is_active",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "effective_timestamp" AS "effective_timestamp",
        "end_timestamp" AS "end_timestamp",
        "date_of_birth" AS "date_of_birth",
        "created_at" AS "created_at",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."scd_table"
      WHERE
        "effective_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        AND "effective_timestamp" < CAST('2022-01-05 00:00:00' AS TIMESTAMP)
    )
    WHERE
      NOT "col_text" IS NULL
  )
), JOINED_PARENTS_ENTITY_UNIVERSE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."cust_id" AS "cust_id",
    REQ."gender" AS "gender"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."cust_id" AS "cust_id",
      R."col_boolean" AS "gender"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TS_COL",
        "POINT_IN_TIME",
        "cust_id"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TS_COL",
          "POINT_IN_TIME",
          "cust_id",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."cust_id"
            FROM "ENTITY_UNIVERSE" AS REQ
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "col_text" AS "__FB_KEY_COL_0",
            "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "POINT_IN_TIME",
            NULL AS "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "is_active" AS "is_active",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "effective_timestamp" AS "effective_timestamp",
              "end_timestamp" AS "end_timestamp",
              "date_of_birth" AS "date_of_birth",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."scd_table"
            WHERE
              NOT "effective_timestamp" IS NULL
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        ANY_VALUE("col_int") AS "col_int",
        ANY_VALUE("col_float") AS "col_float",
        ANY_VALUE("is_active") AS "is_active",
        "col_text",
        ANY_VALUE("col_binary") AS "col_binary",
        ANY_VALUE("col_boolean") AS "col_boolean",
        "effective_timestamp",
        ANY_VALUE("end_timestamp") AS "end_timestamp",
        ANY_VALUE("date_of_birth") AS "date_of_birth",
        ANY_VALUE("created_at") AS "created_at",
        ANY_VALUE("cust_id") AS "cust_id"
      FROM (
        SELECT
          "col_int" AS "col_int",
          "col_float" AS "col_float",
          "is_active" AS "is_active",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          "effective_timestamp" AS "effective_timestamp",
          "end_timestamp" AS "end_timestamp",
          "date_of_birth" AS "date_of_birth",
          "created_at" AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."scd_table"
        WHERE
          NOT "effective_timestamp" IS NULL
      )
      GROUP BY
        "effective_timestamp",
        "col_text"
    ) AS R
      ON L."__FB_LAST_TS" = R."effective_timestamp"
      AND L."__FB_KEY_COL_0" = R."col_text"
      AND (
        L."__FB_TS_COL" < CAST(CONVERT_TIMEZONE('UTC', R."end_timestamp") AS TIMESTAMP)
        OR R."end_timestamp" IS NULL
      )
  ) AS REQ
)
SELECT
  "POINT_IN_TIME",
  "cust_id",
  "gender"
FROM JOINED_PARENTS_ENTITY_UNIVERSE;

CREATE TABLE "sf_db"."sf_schema"."TEMP_LOOKUP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  L."cust_id",
  R."__feature_requiring_parent_serving_V220101__part1",
  R."__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM "TEMP_LOOKUP_UNIVERSE_TABLE_000000000000000000000000" AS L
LEFT JOIN "cat1_gender_1d" AS R
  ON L."gender" = R."gender";

SELECT
  COUNT(*)
FROM "cat1_gender_1d_via_cust_id_000000";

CREATE TABLE "sf_db"."sf_schema"."cat1_gender_1d_via_cust_id_000000" AS
SELECT
  CAST('2022-01-05T00:00:00' AS TIMESTAMP) AS "__feature_timestamp",
  "cust_id",
  "__feature_requiring_parent_serving_V220101__part1",
  "__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM "TEMP_LOOKUP_FEATURE_TABLE_000000000000000000000000";
