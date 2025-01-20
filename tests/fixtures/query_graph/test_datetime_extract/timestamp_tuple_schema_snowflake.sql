SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(
    hour,
    CONVERT_TIMEZONE(
      'UTC',
      CAST(GET("ts", 'timezone') AS VARCHAR),
      TO_TIMESTAMP(CAST(GET("ts", 'timestamp') AS VARCHAR), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    )
  ) AS "hour"
FROM "db"."public"."event_table"
