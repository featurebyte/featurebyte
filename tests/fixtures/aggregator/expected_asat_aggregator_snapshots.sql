WITH "SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_ad5350ffc969140e" AS (
  SELECT
    "POINT_IN_TIME",
    "serving_cust_id",
    DATE_TRUNC('day', "POINT_IN_TIME") AS "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "serving_cust_id"
    FROM "REQUEST_TABLE"
  )
), "SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_ad5350ffc969140e" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "serving_cust_id",
    "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME"
  FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_ad5350ffc969140e"
)
SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1" AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
    DISTINCT_POINT_IN_TIME."serving_cust_id",
    AGGREGATED."_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
  FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_ad5350ffc969140e" AS DISTINCT_POINT_IN_TIME
  LEFT JOIN (
    SELECT
      REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" AS "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME",
      REQ."serving_cust_id" AS "serving_cust_id",
      SUM(SNAPSHOTS."value") AS "_fb_internal_serving_cust_id_as_at_sum_value_cust_id_None_input_1"
    FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_ad5350ffc969140e" AS REQ
    INNER JOIN (
      SELECT
        *
      FROM SNAPSHOTS_TABLE
    ) AS SNAPSHOTS
      ON REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" = SNAPSHOTS."snapshot_datetime_column"
      AND REQ."serving_cust_id" = SNAPSHOTS."cust_id"
    GROUP BY
      REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME",
      REQ."serving_cust_id"
  ) AS AGGREGATED
    ON AGGREGATED."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" = DISTINCT_POINT_IN_TIME."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME"
    AND AGGREGATED."serving_cust_id" = DISTINCT_POINT_IN_TIME."serving_cust_id"
) AS T0
  ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
  AND REQ."serving_cust_id" = T0."serving_cust_id"
