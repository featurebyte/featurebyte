SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  "b" AS "b"
FROM "db"."public"."event_table"
ORDER BY
  RANDOM(1234)
LIMIT 50000
