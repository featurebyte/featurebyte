SELECT
  "cust_id",
  "snapshot_date",
  COUNT(*) AS "COUNT_PER_SNAPSHOT_ID_AND_DATETIME"
FROM (
  SELECT
    *
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "snapshot_date" = TO_CHAR(
      DATEADD(SECOND, 0, DATE_TRUNC('day', DATEADD(SECOND, -604800, SYSDATE()))),
      '%Y-%m-%d'
    )
    AND "cust_id" IS NOT NULL
)
GROUP BY
  "cust_id",
  "snapshot_date"
HAVING
  "COUNT_PER_SNAPSHOT_ID_AND_DATETIME" > 1
LIMIT 10;
