SELECT
  index,
  "cust_id",
  "item_type",
  COUNT(*) AS value_count_d445f47d1ab8d2fa742190a9d0e595f23da0c25d
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      __FB_ENTITY_TABLE_SQL_PLACEHOLDER
    )
    SELECT
      R.*
    FROM __FB_ENTITY_TABLE_NAME
    INNER JOIN (
      SELECT
        L."order_method" AS "order_method",
        R."order_id" AS "order_id",
        R."item_id" AS "item_id",
        R."item_name" AS "item_name",
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
)
GROUP BY
  index,
  "cust_id",
  "item_type"
