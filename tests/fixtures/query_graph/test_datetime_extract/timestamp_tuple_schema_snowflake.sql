SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(
    hour,
    CONVERT_TIMEZONE(
      'UTC',
      CAST(GET(PARSE_JSON("ts"), 'timezone') AS VARCHAR),
      CAST(TO_TIMESTAMP(CAST(GET(PARSE_JSON("ts"), 'timestamp') AS VARCHAR), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS TIMESTAMP)
    )
  ) AS "hour"
FROM "db"."public"."event_table"
