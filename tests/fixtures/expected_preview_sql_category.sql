WITH avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2 AS (
    SELECT *, F_TIMESTAMP_TO_INDEX(  __FB_TILE_START_DATE_COLUMN,  1800,  900,  60) AS "INDEX" FROM (
        SELECT
  TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
  "cust_id",
  "product_type",
  SUM("a") AS sum_value,
  COUNT("a") AS count_value
FROM (
    SELECT
      *,
      FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP))) / 3600) AS tile_index
    FROM (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          ("a" + "b") AS "c"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST('2022-04-18 09:15:00' AS TIMESTAMP)
          AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMP)
    )
)
GROUP BY
  tile_index,
  "cust_id",
  "product_type"
ORDER BY
  tile_index
    )
    ),
REQUEST_TABLE AS (SELECT
  CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS POINT_IN_TIME,
  'C1' AS CUSTOMER_ID),
REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ.CUSTOMER_ID,
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, CUSTOMER_ID FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                7200,
                3600,
                900,
                1800
            )
        )
    ) T
),
REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ.CUSTOMER_ID,
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, CUSTOMER_ID FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                172800,
                3600,
                900,
                1800
            )
        )
    ) T
),
_FB_AGGREGATED AS (SELECT
  REQ.*,
  "T0"."agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2" AS "agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2",
  "T1"."agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2" AS "agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2"
FROM REQUEST_TABLE AS REQ
LEFT JOIN (
    SELECT
      INNER_.POINT_IN_TIME,
      INNER_.CUSTOMER_ID,
      OBJECT_AGG(CASE
        WHEN INNER_.product_type IS NULL THEN '__MISSING__'
        ELSE INNER_.product_type
      END, INNER_."inner_agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2") AS "agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2"
    FROM (
        SELECT
          REQ.POINT_IN_TIME,
          REQ.CUSTOMER_ID,
          TILE.product_type,
          SUM(sum_value) / SUM(count_value) AS "inner_agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2"
        FROM REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID AS REQ
        INNER JOIN avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2 AS TILE
          ON REQ.REQ_TILE_INDEX = TILE.INDEX
          AND REQ.CUSTOMER_ID = TILE.cust_id
        GROUP BY
          REQ.POINT_IN_TIME,
          REQ.CUSTOMER_ID,
          TILE.product_type
    ) AS INNER_
    GROUP BY
      INNER_.POINT_IN_TIME,
      INNER_.CUSTOMER_ID
) AS T0
  ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME
  AND REQ.CUSTOMER_ID = T0.CUSTOMER_ID
LEFT JOIN (
    SELECT
      INNER_.POINT_IN_TIME,
      INNER_.CUSTOMER_ID,
      OBJECT_AGG(CASE
        WHEN INNER_.product_type IS NULL THEN '__MISSING__'
        ELSE INNER_.product_type
      END, INNER_."inner_agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2") AS "agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2"
    FROM (
        SELECT
          REQ.POINT_IN_TIME,
          REQ.CUSTOMER_ID,
          TILE.product_type,
          SUM(sum_value) / SUM(count_value) AS "inner_agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2"
        FROM REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID AS REQ
        INNER JOIN avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2 AS TILE
          ON REQ.REQ_TILE_INDEX = TILE.INDEX
          AND REQ.CUSTOMER_ID = TILE.cust_id
        GROUP BY
          REQ.POINT_IN_TIME,
          REQ.CUSTOMER_ID,
          TILE.product_type
    ) AS INNER_
    GROUP BY
      INNER_.POINT_IN_TIME,
      INNER_.CUSTOMER_ID
) AS T1
  ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME
  AND REQ.CUSTOMER_ID = T1.CUSTOMER_ID)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "agg_w7200_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2" AS "a_2h_average",
  "agg_w172800_avg_f3600_m1800_b900_d62da870cdbe97bbfdb6a7ad61e62089e5f7e1e2" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
