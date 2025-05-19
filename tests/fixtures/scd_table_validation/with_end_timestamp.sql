SELECT
  REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
  SCD."cust_id" AS "cust_id",
  COUNT(*) AS "COUNT_PER_NATURAL_KEY"
FROM (
  SELECT
    SYSDATE() AS "POINT_IN_TIME"
) AS REQ
INNER JOIN (
  SELECT
    "cust_id",
    "effective_date",
    "end_date"
  FROM "my_db"."my_schema"."my_table"
  WHERE
    "cust_id" IS NOT NULL
) AS SCD
  ON SCD."effective_date" <= REQ."POINT_IN_TIME"
  AND (
    SCD."end_date" > REQ."POINT_IN_TIME" OR SCD."end_date" IS NULL
  )
GROUP BY
  REQ."POINT_IN_TIME",
  SCD."cust_id"
HAVING
  "COUNT_PER_NATURAL_KEY" > 1
LIMIT 10;

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
