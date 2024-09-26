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
    *,
    LEAD("effective_date") OVER (PARTITION BY "cust_id" ORDER BY "effective_date") AS "__FB_END_TS"
  FROM (
    SELECT
      "cust_id",
      "effective_date"
    FROM "my_db"."my_schema"."my_table"
  )
) AS SCD
  ON SCD."effective_date" <= REQ."POINT_IN_TIME"
  AND (
    SCD."__FB_END_TS" > REQ."POINT_IN_TIME" OR SCD."__FB_END_TS" IS NULL
  )
GROUP BY
  REQ."POINT_IN_TIME",
  SCD."cust_id"
HAVING
  "COUNT_PER_NATURAL_KEY" > 1
LIMIT 10;
