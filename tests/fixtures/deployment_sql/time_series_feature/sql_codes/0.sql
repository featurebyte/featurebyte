WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      CAST("store_id" AS BIGINT) AS "cust_id"
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
        (
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MONTH, -3, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
        AND (
          TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') >= DATEADD(MONTH, -3, DATEADD(MONTH, -3, DATE_TRUNC('MONTH', {{ CURRENT_TIMESTAMP }})))
          AND TO_TIMESTAMP("date", 'yyyy-mm-DD hh24:mi:ss') < DATEADD(MONTH, -3, DATE_TRUNC('MONTH', {{ CURRENT_TIMESTAMP }}))
        )
    )
    WHERE
      NOT "store_id" IS NULL
  ) AS REQ
), "REQUEST_TABLE_TIME_SERIES_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_cust_id_DISTINCT_BY_POINT_IN_TIME" AS (
  SELECT DISTINCT
    "POINT_IN_TIME" AS "POINT_IN_TIME",
    "cust_id" AS "cust_id",
    "__FB_CRON_JOB_SCHEDULE_DATETIME_0 8 1 * *_Etc/UTC_None" AS "__FB_CRON_JOB_SCHEDULE_DATETIME"
  FROM "DEPLOYMENT_REQUEST_TABLE"
), "REQUEST_TABLE_TIME_SERIES_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_cust_id_DISTINCT_BY_SCHEDULED_JOB_TIME" AS (
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
      "cust_id" AS "cust_id",
      "__FB_CRON_JOB_SCHEDULE_DATETIME_0 8 1 * *_Etc/UTC_None" AS "__FB_CRON_JOB_SCHEDULE_DATETIME"
    FROM "DEPLOYMENT_REQUEST_TABLE"
  )
), "VIEW_b4606d724d31ac42" AS (
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
      "date" >= TO_CHAR(
        DATEADD(MONTH, -1, DATEADD(MONTH, -3, {{ CURRENT_TIMESTAMP }})),
        'YYYY-MM-DD HH24:MI:SS'
      )
      AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
  )
), "VIEW_b4606d724d31ac42_DISTINCT_REFERENCE_DATETIME" AS (
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
    FROM "VIEW_b4606d724d31ac42"
    WHERE
      NOT "date" IS NULL
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1" AS "_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
      DISTINCT_POINT_IN_TIME."cust_id",
      AGGREGATED."_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1"
    FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_cust_id_DISTINCT_BY_POINT_IN_TIME" AS DISTINCT_POINT_IN_TIME
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
          FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_cust_id_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
          INNER JOIN "VIEW_b4606d724d31ac42_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
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
          FROM "REQUEST_TABLE_TIME_SERIES_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_cust_id_DISTINCT_BY_SCHEDULED_JOB_TIME" AS REQ
          INNER JOIN "VIEW_b4606d724d31ac42_DISTINCT_REFERENCE_DATETIME" AS BUCKETED_REFERENCE_DATETIME
            ON FLOOR(REQ."__FB_WINDOW_END_EPOCH" / 3) - 1 = FLOOR(BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" / 3)
          WHERE
            BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" >= REQ."__FB_WINDOW_START_EPOCH"
            AND BUCKETED_REFERENCE_DATETIME."__FB_VIEW_TIMESTAMP_EPOCH" < REQ."__FB_WINDOW_END_EPOCH"
        ) AS RANGE_JOINED
        INNER JOIN "VIEW_b4606d724d31ac42" AS VIEW
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
  AGG."cust_id",
  CAST("_fb_internal_cust_id_time_series_sum_col_float_store_id_None_W3_MONTH_BS1_MONTH_0 8 1 * *_Etc/UTC_None_project_1" AS DOUBLE) AS "col_float_sum_3month",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG