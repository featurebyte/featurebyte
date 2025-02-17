CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id"
FROM ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C
WHERE
  "AGGREGATION_RESULT_NAME" = '_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'
  AND NOT "cust_id" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2022-01-06 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        """cust_id""" AS "cust_id",
        "'_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
      FROM (
        SELECT
          "cust_id",
          "AGGREGATION_RESULT_NAME",
          "VALUE"
        FROM (
          SELECT
            R.*
          FROM (
            SELECT
              "AGGREGATION_RESULT_NAME",
              "LATEST_VERSION"
            FROM (VALUES
              (
                '_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295',
                _fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295_VERSION_PLACEHOLDER
              )) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295')
      )
      PIVOT(MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "__feature_requiring_parent_serving_ttl_V220101__part0"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id",
  T0."__feature_requiring_parent_serving_ttl_V220101__part0"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

CREATE TABLE "sf_db"."sf_schema"."TEMP_LOOKUP_UNIVERSE_TABLE_000000000000000000000000" AS
WITH ENTITY_UNIVERSE AS (
  SELECT
    CAST('2022-01-06 00:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    "transaction_id"
  FROM (
    SELECT DISTINCT
      "col_int" AS "transaction_id"
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "created_at" AS "created_at",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        "event_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        AND "event_timestamp" < CAST('2022-01-06 00:00:00' AS TIMESTAMP)
    )
    WHERE
      NOT "col_int" IS NULL
  )
), JOINED_PARENTS_ENTITY_UNIVERSE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."transaction_id" AS "transaction_id",
    REQ."cust_id" AS "cust_id"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."transaction_id",
      CASE
        WHEN REQ."POINT_IN_TIME" < "T0"."event_timestamp"
        THEN NULL
        ELSE "T0"."cust_id"
      END AS "cust_id"
    FROM ENTITY_UNIVERSE AS REQ
    LEFT JOIN (
      SELECT
        "transaction_id",
        ANY_VALUE("cust_id") AS "cust_id",
        ANY_VALUE("event_timestamp") AS "event_timestamp"
      FROM (
        SELECT
          "col_int" AS "transaction_id",
          "cust_id" AS "cust_id",
          "event_timestamp"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        )
      )
      GROUP BY
        "transaction_id"
    ) AS T0
      ON REQ."transaction_id" = T0."transaction_id"
  ) AS REQ
)
SELECT
  "POINT_IN_TIME",
  "transaction_id",
  "cust_id"
FROM JOINED_PARENTS_ENTITY_UNIVERSE;

CREATE TABLE "sf_db"."sf_schema"."TEMP_LOOKUP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  L."transaction_id",
  R."__feature_requiring_parent_serving_ttl_V220101__part0"
FROM "TEMP_LOOKUP_UNIVERSE_TABLE_000000000000000000000000" AS L
LEFT JOIN "TEMP_FEATURE_TABLE_000000000000000000000000" AS R
  ON L."cust_id" = R."cust_id";

INSERT INTO "cat1_cust_id_30m" (
  "__feature_timestamp",
  "cust_id",
  "__feature_requiring_parent_serving_ttl_V220101__part0"
)
SELECT
  CAST('2022-01-06T00:00:00' AS TIMESTAMP) AS "__feature_timestamp",
  "cust_id",
  "__feature_requiring_parent_serving_ttl_V220101__part0"
FROM "TEMP_FEATURE_TABLE_000000000000000000000000";

INSERT INTO "cat1_cust_id_30m_via_transaction_id_000000" (
  "__feature_timestamp",
  "transaction_id",
  "__feature_requiring_parent_serving_ttl_V220101__part0"
)
SELECT
  CAST('2022-01-06T00:00:00' AS TIMESTAMP) AS "__feature_timestamp",
  "transaction_id",
  "__feature_requiring_parent_serving_ttl_V220101__part0"
FROM "TEMP_LOOKUP_FEATURE_TABLE_000000000000000000000000";
