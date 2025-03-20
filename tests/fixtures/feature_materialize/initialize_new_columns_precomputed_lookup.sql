SELECT
  COUNT(*)
FROM "cat1_gender_1d";

CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  "col_boolean" AS "gender"
FROM (
  SELECT
    "col_int" AS "col_int",
    "col_float" AS "col_float",
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
    AND "effective_timestamp" < CAST('2022-01-01 00:00:00' AS TIMESTAMP)
)
WHERE
  NOT "col_boolean" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."gender",
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), "REQUEST_TABLE_POINT_IN_TIME_gender" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "gender"
  FROM ONLINE_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."gender",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_gender_as_at_count_None_col_boolean_None_project_1" AS "_fb_internal_gender_as_at_count_None_col_boolean_None_project_1"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."gender" AS "gender",
      COUNT(*) AS "_fb_internal_gender_as_at_count_None_col_boolean_None_project_1"
    FROM "REQUEST_TABLE_POINT_IN_TIME_gender" AS REQ
    INNER JOIN (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "effective_timestamp" AS "effective_timestamp",
        "end_timestamp" AS "end_timestamp",
        "date_of_birth" AS "date_of_birth",
        "created_at" AS "created_at",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."scd_table"
    ) AS SCD
      ON REQ."gender" = SCD."col_boolean"
      AND (
        SCD."effective_timestamp" <= REQ."POINT_IN_TIME"
        AND (
          SCD."end_timestamp" > REQ."POINT_IN_TIME" OR SCD."end_timestamp" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."gender"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."gender" = T0."gender"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."gender",
  CAST("_fb_internal_gender_as_at_count_None_col_boolean_None_project_1" AS BIGINT) AS "__feature_requiring_parent_serving_V220101__part1",
  CAST("_fb_internal_gender_as_at_count_None_col_boolean_None_project_1" AS BIGINT) AS "__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."gender",
  T0."__feature_requiring_parent_serving_V220101__part1",
  T0."__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

CREATE TABLE "sf_db"."sf_schema"."TEMP_LOOKUP_UNIVERSE_TABLE_000000000000000000000000" AS
WITH ENTITY_UNIVERSE AS (
  SELECT
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
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
        AND "effective_timestamp" < CAST('2022-01-01 00:00:00' AS TIMESTAMP)
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
            CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."cust_id"
            FROM ENTITY_UNIVERSE AS REQ
          )
          UNION ALL
          SELECT
            CONVERT_TIMEZONE('UTC', "effective_timestamp") AS "__FB_TS_COL",
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
        L."__FB_TS_COL" < CONVERT_TIMEZONE('UTC', R."end_timestamp")
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
LEFT JOIN "TEMP_FEATURE_TABLE_000000000000000000000000" AS R
  ON L."gender" = R."gender";

SELECT
  COUNT(*)
FROM "cat1_gender_1d";

CREATE TABLE "sf_db"."sf_schema"."cat1_gender_1d" AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMP) AS "__feature_timestamp",
  "gender",
  "__feature_requiring_parent_serving_V220101__part1",
  "__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

SELECT
  COUNT(*)
FROM "cat1_gender_1d_via_cust_id_000000";

CREATE TABLE "sf_db"."sf_schema"."cat1_gender_1d_via_cust_id_000000" AS
SELECT
  CAST('2022-01-01T00:00:00' AS TIMESTAMP) AS "__feature_timestamp",
  "cust_id",
  "__feature_requiring_parent_serving_V220101__part1",
  "__feature_requiring_parent_serving_plus_123_V220101__part1"
FROM "TEMP_LOOKUP_FEATURE_TABLE_000000000000000000000000";
