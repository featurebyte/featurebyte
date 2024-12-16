SELECT
  CONVERT_TIMEZONE("tz_col", 'UTC', TO_TIMESTAMP("ts", '%Y-%m-%d %H:%M:%S')) AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a"
FROM "db"."public"."event_table"
