SELECT
  CONVERT_TIMEZONE('Asia/Singapore', 'UTC', DATEADD(second, 86400, "ts")) AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a"
FROM "db"."public"."event_table"