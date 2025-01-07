WITH "REQUEST_TABLE_TIME_SERIES_W3_MONTH_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME",
    (
      (
        EXTRACT(year FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1970
      ) * 12
    ) + EXTRACT(month FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1 - 3 AS "__FB_WINDOW_START_EPOCH",
    (
      (
        EXTRACT(year FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1970
      ) * 12
    ) + EXTRACT(month FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1 AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC"
  )
), "VIEW_5dc86c5d7219d8e6" AS (
  SELECT
    *,
    (
      (
        EXTRACT(year FROM TO_TIMESTAMP("snapshot_date", 'YYYYMMDD')) - 1970
      ) * 12
    ) + EXTRACT(month FROM TO_TIMESTAMP("snapshot_date", 'YYYYMMDD')) - 1 AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT
      "snapshot_date" AS "snapshot_date",
      "cust_id" AS "cust_id",
      "a" AS "a"
    FROM "db"."public"."customer_snapshot"
  )
)
SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_input_1" AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "CUSTOMER_ID" AS "CUSTOMER_ID",
    SUM("a") AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_input_1"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
      VIEW."a"
    FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS REQ
    INNER JOIN "VIEW_5dc86c5d7219d8e6" AS VIEW
      ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
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
    FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_0 0 * * *_Etc/UTC_CUSTOMER_ID" AS REQ
    INNER JOIN "VIEW_5dc86c5d7219d8e6" AS VIEW
      ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
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
