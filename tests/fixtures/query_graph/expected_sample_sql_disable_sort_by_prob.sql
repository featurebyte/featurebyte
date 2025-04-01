SELECT
  "ts",
  "cust_id",
  "a",
  "b",
  "a_copy"
FROM (
  SELECT
    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "ts",
    "cust_id",
    "a",
    "b",
    "a_copy"
  FROM (
    SELECT
      "ts" AS "ts",
      CAST("cust_id" AS VARCHAR) AS "cust_id",
      "a" AS "a",
      "b" AS "b",
      "a" AS "a_copy"
    FROM "db"."public"."event_table"
  )
)
WHERE
  "prob" <= 0.15000000000000002
LIMIT 1000
