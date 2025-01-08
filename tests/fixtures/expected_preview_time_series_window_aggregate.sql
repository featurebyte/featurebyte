WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME",
    DATE_PART(EPOCH_SECOND, DATE_TRUNC('day', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 604800 AS "__FB_WINDOW_START_EPOCH",
    DATE_PART(EPOCH_SECOND, DATE_TRUNC('day', "__FB_CRON_JOB_SCHEDULE_DATETIME")) AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC"
  )
), "VIEW_1d8f77595633310e" AS (
  SELECT
    *,
    DATE_PART(EPOCH_SECOND, TO_TIMESTAMP("snapshot_date", 'YYYYMMDD')) AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT
      "snapshot_date" AS "snapshot_date",
      "cust_id" AS "cust_id",
      "a" AS "a"
    FROM "db"."public"."customer_snapshot"
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_input_1" AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME" AS "POINT_IN_TIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_input_1"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_1d8f77595633310e" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS REQ
      INNER JOIN "VIEW_1d8f77595633310e" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_input_1" AS DOUBLE) AS "a_7d_sum"
FROM _FB_AGGREGATED AS AGG
