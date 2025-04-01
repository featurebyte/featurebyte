SELECT
  "ts" AS "ts",
  CAST("cust_id" AS VARCHAR) AS "cust_id",
  "a" AS "a",
  "b" AS "b",
  "a" AS "a_copy"
FROM "db"."public"."event_table"
LIMIT 1000
