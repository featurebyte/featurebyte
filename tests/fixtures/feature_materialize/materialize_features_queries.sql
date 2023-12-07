CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  "cust_id"
FROM online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    SYSDATE() AS POINT_IN_TIME
  FROM (
    SELECT
      *
    FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000"
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "cust_id",
      "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
    FROM (
      SELECT
        """cust_id""" AS "cust_id",
        "'_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481'" AS "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481"
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
              ('_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481', _fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481_VERSION_PLACEHOLDER)) AS version_table("AGGREGATION_RESULT_NAME", "LATEST_VERSION")
          ) AS L
          INNER JOIN online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c AS R
            ON R."AGGREGATION_RESULT_NAME" = L."AGGREGATION_RESULT_NAME"
            AND R."VERSION" = L."LATEST_VERSION"
        )
        WHERE
          "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481')
      )   PIVOT(  MAX("VALUE") FOR "AGGREGATION_RESULT_NAME" IN ('_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481'))
    )
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."cust_id",
  "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" AS "sum_30m"
FROM _FB_AGGREGATED AS AGG;
