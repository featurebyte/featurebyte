WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a.INDEX,
    avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a."cust_id",
    sum_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a,
    count_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-03-21 09:15:00' AS TIMESTAMP)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("ord_size") AS sum_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a,
        COUNT("ord_size") AS count_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-03-21 09:15:00' AS TIMESTAMP))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            L."ts" AS "ts",
            L."cust_id" AS "cust_id",
            L."order_id" AS "order_id",
            L."order_method" AS "order_method",
            R."__FB_TEMP_FEATURE_NAME" AS "ord_size"
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "order_id" AS "order_id",
                "order_method" AS "order_method"
              FROM "db"."public"."event_table"
            )
            WHERE
              "ts" >= CAST('2022-03-21 09:15:00' AS TIMESTAMP)
              AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMP)
          ) AS L
          LEFT JOIN (
            SELECT
              (
                "order_size" + 123
              ) AS "__FB_TEMP_FEATURE_NAME",
              "order_id"
            FROM (
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
            )
          ) AS R
            ON L."order_id" = R."order_id"
        )
      )
      GROUP BY
        tile_index,
        "cust_id"
      ORDER BY
        tile_index
    )
  ) AS avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 720 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."agg_w2592000_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a" AS "agg_w2592000_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a) / SUM(count_value_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a) AS "agg_w2592000_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a"
    FROM "REQUEST_TABLE_W2592000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 720) = FLOOR(TILE.INDEX / 720)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 720) - 1 = FLOOR(TILE.INDEX / 720)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "agg_w2592000_avg_b88dc6c07aae0c36bef764588de5f5e6df18be7a" AS "order_size_30d_avg"
FROM _FB_AGGREGATED AS AGG
