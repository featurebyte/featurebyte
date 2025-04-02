WITH "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME"
  FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
), "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS (
  SELECT
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME",
    DATE_PART(EPOCH_SECOND, DATE_TRUNC('day', "__FB_CRON_JOB_SCHEDULE_DATETIME")) - 604800 AS "__FB_WINDOW_START_EPOCH",
    DATE_PART(EPOCH_SECOND, DATE_TRUNC('day', "__FB_CRON_JOB_SCHEDULE_DATETIME")) AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "CUSTOMER_ID",
      "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
  )
), "REQUEST_TABLE_TIME_SERIES_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME"
  FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
), "REQUEST_TABLE_TIME_SERIES_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS (
  SELECT
    "CUSTOMER_ID",
    "__FB_CRON_JOB_SCHEDULE_DATETIME",
    DATE_PART(
      EPOCH_SECOND,
      DATE_TRUNC('day', DATE_ADD("__FB_CRON_JOB_SCHEDULE_DATETIME", -3, 'DAY'))
    ) - 604800 AS "__FB_WINDOW_START_EPOCH",
    DATE_PART(
      EPOCH_SECOND,
      DATE_TRUNC('day', DATE_ADD("__FB_CRON_JOB_SCHEDULE_DATETIME", -3, 'DAY'))
    ) AS "__FB_WINDOW_END_EPOCH"
  FROM (
    SELECT DISTINCT
      "CUSTOMER_ID",
      "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "REQUEST_TABLE_0 0 * * *_Etc/UTC_Asia/Singapore"
  )
), "VIEW_62d7bb7b6b3ca3a1" AS (
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
), "VIEW_77345e69cdbdcd9c" AS (
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
)
SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1" AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1",
  "T1"."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1" AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
    DISTINCT_POINT_IN_TIME."CUSTOMER_ID",
    AGGREGATED."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
  FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS DISTINCT_POINT_IN_TIME
  LEFT JOIN (
    SELECT
      "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
    FROM (
      SELECT
        REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
      INNER JOIN "VIEW_62d7bb7b6b3ca3a1" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      UNION ALL
      SELECT
        REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
      INNER JOIN "VIEW_62d7bb7b6b3ca3a1" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
    )
    GROUP BY
      "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID"
  ) AS AGGREGATED
    ON AGGREGATED."__FB_CRON_JOB_SCHEDULE_DATETIME" = DISTINCT_POINT_IN_TIME."__FB_CRON_JOB_SCHEDULE_DATETIME"
    AND AGGREGATED."CUSTOMER_ID" = DISTINCT_POINT_IN_TIME."CUSTOMER_ID"
) AS T0
  ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
LEFT JOIN (
  SELECT
    DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
    DISTINCT_POINT_IN_TIME."CUSTOMER_ID",
    AGGREGATED."_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
  FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_POINT_IN_TIME" AS DISTINCT_POINT_IN_TIME
  LEFT JOIN (
    SELECT
      "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID" AS "CUSTOMER_ID",
      SUM("a") AS "_fb_internal_CUSTOMER_ID_time_series_sum_a_cust_id_None_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_input_1"
    FROM (
      SELECT
        REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
      INNER JOIN "VIEW_77345e69cdbdcd9c" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      UNION ALL
      SELECT
        REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
        REQ."CUSTOMER_ID",
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
        VIEW."a"
      FROM "REQUEST_TABLE_TIME_SERIES_W7_DAY_BS3_DAY_0 0 * * *_Etc/UTC_Asia/Singapore_CUSTOMER_ID_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
      INNER JOIN "VIEW_77345e69cdbdcd9c" AS VIEW
        ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 604800) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 604800)
        AND REQ."CUSTOMER_ID" = VIEW."cust_id"
      WHERE
        VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
        AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
    )
    GROUP BY
      "__FB_CRON_JOB_SCHEDULE_DATETIME",
      "CUSTOMER_ID"
  ) AS AGGREGATED
    ON AGGREGATED."__FB_CRON_JOB_SCHEDULE_DATETIME" = DISTINCT_POINT_IN_TIME."__FB_CRON_JOB_SCHEDULE_DATETIME"
    AND AGGREGATED."CUSTOMER_ID" = DISTINCT_POINT_IN_TIME."CUSTOMER_ID"
) AS T1
  ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
