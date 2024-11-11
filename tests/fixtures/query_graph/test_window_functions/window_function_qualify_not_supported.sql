SELECT
  "ts",
  "cust_id",
  "a",
  "b",
  "prev_a"
FROM (
  SELECT
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "a" AS "a",
    "b" AS "b",
    LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a",
    (
      LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
    ) AS "__fb_qualify_condition_column"
  FROM "db"."public"."event_table"
  WHERE
    (
      "a" = 123
    )
)
WHERE
  "__fb_qualify_condition_column"
