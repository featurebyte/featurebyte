SELECT
  "effective_date",
  "cust_id",
  COUNT(*) AS "COUNT_PER_NATURAL_KEY"
FROM (
  SELECT
    "cust_id",
    "effective_date"
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "cust_id" IS NOT NULL
)
GROUP BY
  "effective_date",
  "cust_id"
HAVING
  "COUNT_PER_NATURAL_KEY" > 1
LIMIT 10;
