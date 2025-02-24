SELECT
  (
    R."item_id" + 123
  )
FROM (
  SELECT
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "order_id" AS "order_id",
    "order_method" AS "order_method"
  FROM "db"."public"."event_table"
) AS L
INNER JOIN (
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
