WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      L."ts" AS "ts",
      L."cust_id" AS "cust_id",
      R."item_type" AS "item_type"
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
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  "item_type",
  COUNT(*) AS value_count_1ce4ed1665a2441405f198edee6e7736e74d0b5a
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id",
  "item_type"
