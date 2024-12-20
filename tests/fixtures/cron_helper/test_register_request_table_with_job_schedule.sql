CREATE TABLE "sf_db"."sf_schema"."request_table_cron_schedule_1" AS
SELECT
  L."POINT_IN_TIME" AS "POINT_IN_TIME",
  L."SERIES_ID" AS "SERIES_ID",
  R."__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_CRON_JOB_SCHEDULE_DATETIME"
FROM (
  SELECT
    "__FB_LAST_TS",
    "POINT_IN_TIME",
    "SERIES_ID"
  FROM (
    SELECT
      LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
      "POINT_IN_TIME",
      "SERIES_ID",
      "__FB_EFFECTIVE_TS_COL"
    FROM (
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
        NULL AS "__FB_EFFECTIVE_TS_COL",
        2 AS "__FB_TS_TIE_BREAKER_COL",
        "POINT_IN_TIME" AS "POINT_IN_TIME",
        "SERIES_ID" AS "SERIES_ID"
      FROM "request_table"
      UNION ALL
      SELECT
        CAST(CONVERT_TIMEZONE('UTC', "__FB_CRON_JOB_SCHEDULE_DATETIME") AS TIMESTAMP) AS "__FB_TS_COL",
        "__FB_CRON_JOB_SCHEDULE_DATETIME" AS "__FB_EFFECTIVE_TS_COL",
        1 AS "__FB_TS_TIE_BREAKER_COL",
        NULL AS "POINT_IN_TIME",
        NULL AS "SERIES_ID"
      FROM "cron_schedule_1"
    )
  )
  WHERE
    "__FB_EFFECTIVE_TS_COL" IS NULL
) AS L
LEFT JOIN "cron_schedule_1" AS R
  ON L."__FB_LAST_TS" = R."__FB_CRON_JOB_SCHEDULE_DATETIME";
