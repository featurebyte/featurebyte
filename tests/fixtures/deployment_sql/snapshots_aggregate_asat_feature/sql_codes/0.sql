WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."another_key",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      "col_binary" AS "another_key"
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
      FROM "sf_database"."sf_schema"."snapshots_table"
      WHERE
        (
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MINUTE, -4320, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
        AND (
          "date" >= TO_CHAR(
            DATEADD(DAY, -7, CAST('1970-01-01 00:00:00' AS TIMESTAMP)),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(DAY, 7, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
        )
    )
    WHERE
      NOT "col_binary" IS NULL
  ) AS REQ
), "SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_c483970cb69d272b" AS (
  SELECT
    "POINT_IN_TIME",
    "another_key",
    TO_CHAR(
      DATEADD(
        SECOND,
        -259200,
        DATEADD(
          SECOND,
          -86400,
          DATE_TRUNC('day', "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Etc/UTC_Etc/UTC")
        )
      ),
      'YYYY-MM-DD HH24:MI:SS'
    ) AS "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "__FB_CRON_JOB_SCHEDULE_DATETIME_0 0 * * *_Etc/UTC_Etc/UTC",
      "another_key"
    FROM "DEPLOYMENT_REQUEST_TABLE"
  )
), "SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_c483970cb69d272b" AS (
  SELECT DISTINCT
    "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME",
    "another_key"
  FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_c483970cb69d272b"
), _FB_AGGREGATED AS (
  SELECT
    REQ."another_key",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1" AS "_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      DISTINCT_POINT_IN_TIME."POINT_IN_TIME",
      DISTINCT_POINT_IN_TIME."another_key",
      AGGREGATED."_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1"
    FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_c483970cb69d272b" AS DISTINCT_POINT_IN_TIME
    LEFT JOIN (
      SELECT
        REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" AS "__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME",
        REQ."another_key" AS "another_key",
        SUM(SNAPSHOTS."col_float") AS "_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1"
      FROM "SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_c483970cb69d272b" AS REQ
      INNER JOIN (
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
        FROM "sf_database"."sf_schema"."snapshots_table"
        WHERE
          "date" >= TO_CHAR(
            DATEADD(MONTH, -1, DATEADD(MINUTE, -4320, {{ CURRENT_TIMESTAMP }})),
            'YYYY-MM-DD HH24:MI:SS'
          )
          AND "date" <= TO_CHAR(DATEADD(MONTH, 1, {{ CURRENT_TIMESTAMP }}), 'YYYY-MM-DD HH24:MI:SS')
      ) AS SNAPSHOTS
        ON REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" = SNAPSHOTS."date"
        AND REQ."another_key" = SNAPSHOTS."col_binary"
      GROUP BY
        REQ."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME",
        REQ."another_key"
    ) AS AGGREGATED
      ON AGGREGATED."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME" = DISTINCT_POINT_IN_TIME."__FB_SNAPSHOTS_ADJUSTED_POINT_IN_TIME"
      AND AGGREGATED."another_key" = DISTINCT_POINT_IN_TIME."another_key"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."another_key" = T0."another_key"
)
SELECT
  AGG."another_key",
  CAST("_fb_internal_another_key_as_at_sum_col_float_col_binary_None_project_1" AS DOUBLE) AS "snapshots_asat_col_int_sum",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG