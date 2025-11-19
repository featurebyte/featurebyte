SELECT
  "partition_col" AS "partition_col",
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  "user_city" AS "user_city",
  "user_country_code" AS "user_country_code"
FROM (
  SELECT
    "partition_col" AS "partition_col",
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "a" AS "a",
    CAST(GET(GET("user_info", 'address'), 'city') AS VARCHAR) AS "user_city",
    CAST(GET(GET(GET("user_info", 'address'), 'billing_address'), 'country_code') AS VARCHAR) AS "user_country_code"
  FROM "db"."public"."event_table"
)
WHERE
  "partition_col" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
  AND "partition_col" <= TO_CHAR(CAST('2023-06-01 00:00:00' AS TIMESTAMP), '%Y-%m-%d')
