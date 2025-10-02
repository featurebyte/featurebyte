SELECT
  "cust_id",
  "snapshot_date",
  COUNT(*) AS "COUNT_PER_SNAPSHOT_ID_AND_DATETIME"
FROM (
  SELECT
    "cust_id",
    "snapshot_date"
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "cust_id" IS NOT NULL
)
GROUP BY
  "cust_id",
  "snapshot_date"
HAVING
  "COUNT_PER_SNAPSHOT_ID_AND_DATETIME" > 1
LIMIT 10;
