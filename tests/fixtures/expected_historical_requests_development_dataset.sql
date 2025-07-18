CREATE TABLE "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_point_in_time" AS
SELECT DISTINCT
  "POINT_IN_TIME",
  "cust_id",
  "__FB_CRON_JOB_SCHEDULE_DATETIME"
FROM "REQUEST_TABLE_0 8 1 * *_Etc/UTC_None";

CREATE TABLE "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS
SELECT
  "cust_id",
  "__FB_CRON_JOB_SCHEDULE_DATETIME",
  (
    (
      DATE_PART(year, DATE_TRUNC('month', DATEADD(MONTH, -1, "__FB_CRON_JOB_SCHEDULE_DATETIME"))) - 1970
    ) * 12
  ) + DATE_PART(month, DATE_TRUNC('month', DATEADD(MONTH, -1, "__FB_CRON_JOB_SCHEDULE_DATETIME"))) - 1 - 3 AS "__FB_WINDOW_START_EPOCH",
  (
    (
      DATE_PART(year, DATE_TRUNC('month', DATEADD(MONTH, -1, "__FB_CRON_JOB_SCHEDULE_DATETIME"))) - 1970
    ) * 12
  ) + DATE_PART(month, DATE_TRUNC('month', DATEADD(MONTH, -1, "__FB_CRON_JOB_SCHEDULE_DATETIME"))) - 1 AS "__FB_WINDOW_END_EPOCH"
FROM (
  SELECT DISTINCT
    "cust_id",
    "__FB_CRON_JOB_SCHEDULE_DATETIME"
  FROM "REQUEST_TABLE_0 8 1 * *_Etc/UTC_None"
);

CREATE TABLE "__TEMP_0" AS
WITH "VIEW_3eb0e88b24e00e8d_BUCKET_COLUMN" AS (
  SELECT
    *,
    (
      (
        DATE_PART(year, TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS')) - 1970
      ) * 12
    ) + DATE_PART(month, TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS')) - 1 AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      "date" AS "date",
      "store_id" AS "store_id",
      "another_timestamp_col" AS "another_timestamp_col"
    FROM "db"."schema"."sf_time_series_table_dev_sampled"
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_4" AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_4"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
      DISTINCT_POINT_IN_TIME."cust_id",
      AGGREGATED."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_4"
    FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_point_in_time" AS DISTINCT_POINT_IN_TIME
    LEFT JOIN (
      SELECT
        "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME",
        "cust_id" AS "cust_id",
        SUM("col_float") AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_4"
      FROM (
        SELECT
          REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
          REQ."cust_id",
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
          VIEW."col_float"
        FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS REQ
        INNER JOIN "VIEW_3eb0e88b24e00e8d_BUCKET_COLUMN" AS VIEW
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
          AND REQ."cust_id" = VIEW."store_id"
        WHERE
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
        UNION ALL
        SELECT
          REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
          REQ."cust_id",
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH",
          VIEW."col_float"
        FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS REQ
        INNER JOIN "VIEW_3eb0e88b24e00e8d_BUCKET_COLUMN" AS VIEW
          ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) - 1 = FLOOR(VIEW."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
          AND REQ."cust_id" = VIEW."store_id"
        WHERE
          VIEW."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
          AND VIEW."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
      )
      GROUP BY
        "__FB_CRON_JOB_SCHEDULE_DATETIME",
        "cust_id"
    ) AS AGGREGATED
      ON AGGREGATED."__FB_CRON_JOB_SCHEDULE_DATETIME" = DISTINCT_POINT_IN_TIME."__FB_CRON_JOB_SCHEDULE_DATETIME"
      AND AGGREGATED."cust_id" = DISTINCT_POINT_IN_TIME."cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_4" AS DOUBLE) AS "col_float_sum_3month"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "SOME_HISTORICAL_FEATURE_TABLE" AS
SELECT
  REQ."__FB_TABLE_ROW_INDEX",
  REQ."POINT_IN_TIME",
  REQ."CUSTOMER_ID",
  T0."col_float_sum_3month"
FROM "REQUEST_TABLE" AS REQ
LEFT JOIN "__TEMP_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX"
