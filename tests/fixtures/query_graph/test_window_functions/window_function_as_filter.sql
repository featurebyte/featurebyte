SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  "b" AS "b",
  LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a"
FROM "db"."public"."event_table"
QUALIFY
  (
    LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
  )
