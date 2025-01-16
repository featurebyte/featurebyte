SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(hour, "ts") AS "hour"
FROM "db"."public"."event_table"
