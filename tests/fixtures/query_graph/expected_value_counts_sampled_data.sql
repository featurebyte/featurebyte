SELECT
  "ts",
  "cust_id",
  "a",
  "b"
FROM (
  SELECT
    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
    "ts",
    "cust_id",
    "a",
    "b"
  FROM (
    SELECT
      "ts" AS "ts",
      "cust_id" AS "cust_id",
      "a" AS "a",
      "b" AS "b"
    FROM "db"."public"."event_table"
  )
)
WHERE
  "prob" <= 0.75
ORDER BY
  "prob"
LIMIT 50000
