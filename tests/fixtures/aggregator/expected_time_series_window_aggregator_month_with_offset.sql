WITH "REQUEST_TABLE_TIME_SERIES_W3_MONTH_O1_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME"
  FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
), "REQUEST_TABLE_TIME_SERIES_W3_MONTH_O1_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS (
  SELECT
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME",
    (
      (
        EXTRACT(year FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1970
      ) * 12
    ) + EXTRACT(month FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1 - 1 - 3 AS "__FB_WINDOW_START_EPOCH",
    (
      (
        EXTRACT(year FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1970
      ) * 12
    ) + EXTRACT(month FROM DATE_TRUNC('month', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 1 - 1 AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "CUSTOMER_ID",
      "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
  )
), "VIEW_9695f8244f6fcfef" AS (
  SELECT
    *
  FROM (
    SELECT
      "snapshot_date" AS "snapshot_date",
      "cust_id" AS "cust_id",
      "a" AS "a"
    FROM "db"."public"."customer_snapshot"
  )
), "VIEW_9695f8244f6fcfef_DISTINCT_REFERENCE_DATETIME" AS (
  SELECT
    "snapshot_date" AS "__FB_VIEW_REFERENCE_DATETIME",
    (
      (
        EXTRACT(year FROM TO_TIMESTAMP("snapshot_date", 'YYYYMMDD')) - 1970
      ) * 12
    ) + EXTRACT(month FROM TO_TIMESTAMP("snapshot_date", 'YYYYMMDD')) - 1 AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT DISTINCT
      "snapshot_date"
    FROM "VIEW_9695f8244f6fcfef"
    WHERE
      NOT "snapshot_date" IS NULL
  )
)
SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_input_1" AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
    DISTINCT_POINT_IN_TIME."CUSTOMER_ID",
    AGGREGATED."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
  FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_O1_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS DISTINCT_POINT_IN_TIME
  LEFT JOIN (
    SELECT
      "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W3_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
    FROM (
      SELECT
        RANGE_JOINED."__FB_CRON_JOB_SCHEDULE_DATETIME",
        RANGE_JOINED."__FB_VIEW_TIMESTAMP_EPOCH",
        RANGE_JOINED."CUSTOMER_ID",
        VIEW."a"
      FROM (
        SELECT
          REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
          REQ."CUSTOMER_ID",
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_REFERENCE_DATETIME",
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH"
        FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_O1_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
        INNER JOIN "VIEW_9695f8244f6fcfef_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) = FLOOR(BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
        WHERE
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
        UNION ALL
        SELECT
          REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
          REQ."CUSTOMER_ID",
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_REFERENCE_DATETIME",
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH"
        FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_O1_MONTH_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
        INNER JOIN "VIEW_9695f8244f6fcfef_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) - 1 = FLOOR(BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
        WHERE
          BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      ) AS RANGE_JOINED
      INNER JOIN "VIEW_9695f8244f6fcfef" AS VIEW
        ON RANGE_JOINED."__FB_VIEW_REFERENCE_DATETIME" = VIEW."snapshot_date"
        AND RANGE_JOINED."CUSTOMER_ID" = VIEW."cust_id"
    )
    GROUP BY
      "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID"
  ) AS AGGREGATED
    ON AGGREGATED."__FB_CRON_JOB_SCHEDULE_DATETIME" = DISTINCT_POINT_IN_TIME."__FB_CRON_JOB_SCHEDULE_DATETIME"
    AND AGGREGATED."CUSTOMER_ID" = DISTINCT_POINT_IN_TIME."CUSTOMER_ID"
) AS T0
  ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
