WITH MY_REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS (
    SELECT
      "CUSTOMER_ID",
      "order_id",
      T0."a_48h_average",
      SYSDATE() AS POINT_IN_TIME
    FROM "MY_REQUEST_TABLE" AS REQ
    LEFT JOIN online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6 AS T0
      ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
), "REQUEST_TABLE_order_id" AS (
    SELECT DISTINCT
      "order_id"
    FROM MY_REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP
), _FB_AGGREGATED AS (
    SELECT
      REQ."CUSTOMER_ID",
      REQ."order_id",
      REQ."a_48h_average",
      "T0"."order_size" AS "order_size"
    FROM MY_REQUEST_TABLE_POST_FEATURE_STORE_LOOKUP AS REQ
    LEFT JOIN (
        SELECT
          ITEM_AGG."order_size" AS "order_size",
          REQ."order_id" AS "order_id"
        FROM "REQUEST_TABLE_order_id" AS REQ
        INNER JOIN (
            SELECT
              "order_id",
              COUNT(*) AS "order_size"
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
        ) AS ITEM_AGG
          ON REQ."order_id" = ITEM_AGG."order_id"
    ) AS T0
      ON REQ."order_id" = T0."order_id"
)
SELECT
  AGG."CUSTOMER_ID",
  AGG."order_id",
  AGG."a_48h_average",
  "order_size" AS "order_size"
FROM _FB_AGGREGATED AS AGG
