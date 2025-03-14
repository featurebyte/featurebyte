SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(hour, TO_TIMESTAMP("ts", '%Y-%m-%d %H:%M:%S')) AS "hour"
FROM "db"."public"."event_table"
