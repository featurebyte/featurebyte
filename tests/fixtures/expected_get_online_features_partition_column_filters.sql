CREATE TABLE "__temp_feature_query_000000000000000000000000_online_request_table_0_8_1_any_any_etc_utc_none" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2023-10-01 12:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "req_db_name"."req_schema_name"."req_table_name" AS REQ
)
SELECT
  L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
  L."cust_id" AS "cust_id",
  L."POINT_IN_TIME" AS "POINT_IN_TIME",
  R."__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME"
FROM (
  SELECT
    "__FB_LAST_TS",
    "__FB_TABLE_ROW_INDEX",
    "cust_id",
    "POINT_IN_TIME"
  FROM (
    SELECT
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
      "__FB_TABLE_ROW_INDEX",
      "cust_id",
      "POINT_IN_TIME",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        0 AS "__FB_TS_TIE_BREAKER_COL",
        "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
        "cust_id" AS "cust_id",
        "POINT_IN_TIME" AS "POINT_IN_TIME"
      FROM "ONLINE_REQUEST_TABLE"
      UNION ALL
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "__FB_CRON_JOB_SCHEDULE_DATETIME_UTC") AS TIMESTAMP) AS "__FB_TS_COL",
        "__FB_CRON_JOB_SCHEDULE_DATETIME_UTC" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "__FB_TABLE_ROW_INDEX",
        NULL AS "cust_id",
        NULL AS "POINT_IN_TIME"
      FROM "__temp_cron_job_schedule_000000000000000000000000"
    )
  )
  WHERE
    "__FB_EFFECTIVE_TS_COL" IS NULL
) AS L
LEFT JOIN "__temp_cron_job_schedule_000000000000000000000000" AS R
  ON L."__FB_LAST_TS" = R."__FB_CRON_JOB_SCHEDULE_DATETIME_UTC";

CREATE TABLE "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_point_in_time" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2023-10-01 12:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "req_db_name"."req_schema_name"."req_table_name" AS REQ
)
SELECT DISTINCT
  "POINT_IN_TIME",
  "cust_id",
  "__FB_CRON_JOB_SCHEDULE_DATETIME"
FROM "__temp_feature_query_000000000000000000000000_online_request_table_0_8_1_any_any_etc_utc_none";

CREATE TABLE "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2023-10-01 12:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "req_db_name"."req_schema_name"."req_table_name" AS REQ
)
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
  FROM "__temp_feature_query_000000000000000000000000_online_request_table_0_8_1_any_any_etc_utc_none"
);

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    CAST('2023-10-01 12:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "req_db_name"."req_schema_name"."req_table_name" AS REQ
), "VIEW_6dddd417148232aa" AS (
  SELECT
    *
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
    FROM "sf_database"."sf_schema"."time_series_table"
    WHERE
      "date" >= TO_CHAR(CAST('2023-04-01 12:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')
      AND "date" <= TO_CHAR(CAST('2024-01-01 12:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')
  )
), "VIEW_6dddd417148232aa_DISTINCT_REFERENCE_DATETIME" AS (
  SELECT
    "date" AS "__FB_VIEW_REFERENCE_DATETIME",
    (
      (
        DATE_PART(year, TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS')) - 1970
      ) * 12
    ) + DATE_PART(month, TO_TIMESTAMP("date", 'YYYY-MM-DD HH24:MI:SS')) - 1 AS "__FB_VIEW_TIMESTAMP_EPOCH"
  FROM (
    SELECT DISTINCT
      "date"
    FROM "VIEW_6dddd417148232aa"
    WHERE
      NOT "date" IS NULL
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1" AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
      DISTINCT_POINT_IN_TIME."cust_id",
      AGGREGATED."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1"
    FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_point_in_time" AS DISTINCT_POINT_IN_TIME
    LEFT JOIN (
      SELECT
        "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME",
        "cust_id" AS "cust_id",
        SUM("col_float") AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1"
      FROM (
        SELECT
          RANGE_JOINED."__FB_CRON_JOB_SCHEDULE_DATETIME",
          RANGE_JOINED."__FB_VIEW_TIMESTAMP_EPOCH",
          RANGE_JOINED."cust_id",
          VIEW."col_float"
        FROM (
          SELECT
            REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
            REQ."cust_id",
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_REFERENCE_DATETIME",
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH"
          FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS REQ
          INNER JOIN "VIEW_6dddd417148232aa_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
            ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) = FLOOR(BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
          WHERE
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
            AND BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
          UNION ALL
          SELECT
            REQ."__FB_CRON_JOB_SCHEDULE_DATETIME",
            REQ."cust_id",
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_REFERENCE_DATETIME",
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH"
          FROM "__temp_feature_query_000000000000000000000000_request_table_time_series_w3_month_bs1_month_0_8_1_any_any_etc_utc_none_cust_id_distinct_by_scheduled_job_time" AS REQ
          INNER JOIN "VIEW_6dddd417148232aa_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
            ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) - 1 = FLOOR(BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
          WHERE
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
            AND BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
        ) AS RANGE_JOINED
        INNER JOIN "VIEW_6dddd417148232aa" AS VIEW
          ON RANGE_JOINED."__FB_VIEW_REFERENCE_DATETIME" = VIEW."date"
          AND RANGE_JOINED."cust_id" = VIEW."store_id"
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
  AGG."cust_id",
  CAST("_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1" AS DOUBLE) AS "col_float_sum_3month"
FROM _FB_AGGREGATED AS AGG;

SELECT
  COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
FROM "__TEMP_000000000000000000000000_0";

CREATE TABLE "some_database"."some_schema"."some_table" AS
SELECT
  REQ."cust_id",
  T0."col_float_sum_3month"
FROM "req_table_name" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";
