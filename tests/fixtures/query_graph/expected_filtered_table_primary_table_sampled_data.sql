SELECT
  CAST(L."order_method" AS VARCHAR) AS "order_method",
  L."cust_id" AS "cust_id",
  L."ts" AS "ts",
  R."order_id" AS "order_id",
  R."item_id" AS "item_id",
  CAST(R."item_name" AS VARCHAR) AS "item_name",
  CAST(R."item_type" AS VARCHAR) AS "item_type"
FROM (
  SELECT
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "order_id" AS "order_id",
    "order_method" AS "order_method"
  FROM "db"."public"."event_table"
) AS L
LEFT JOIN (
  SELECT
    "order_id",
    ANY_VALUE("item_id") AS "item_id",
    ANY_VALUE("item_name") AS "item_name",
    ANY_VALUE("item_type") AS "item_type"
  FROM (
    SELECT
      "order_id" AS "order_id",
      "item_id" AS "item_id",
      "item_name" AS "item_name",
      "item_type" AS "item_type"
    FROM "db"."public"."item_table"
  )
  GROUP BY
    "order_id"
) AS R
  ON L."order_id" = R."order_id"
WHERE
  (
    R."order_id" = 1
  )
LIMIT 10
