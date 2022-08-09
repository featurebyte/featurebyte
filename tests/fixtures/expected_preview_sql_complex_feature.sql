WITH avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f AS (
    SELECT *, F_TIMESTAMP_TO_INDEX(  __FB_TILE_START_DATE_COLUMN,  1800,  900,  60) AS "INDEX" FROM (
        SELECT
  TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
  "cust_id",
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
  "cust_id"
ORDER BY
  tile_index
    )
    ),
sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1 AS (
    SELECT *, F_TIMESTAMP_TO_INDEX(  __FB_TILE_START_DATE_COLUMN,  1800,  900,  60) AS "INDEX" FROM (
        SELECT
  TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST('2022-04-13 09:15:00' AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
  "biz_id",
  SUM("a") AS value
FROM (
    SELECT
      *,
      FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-13 09:15:00' AS TIMESTAMP))) / 3600) AS tile_index
    FROM (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          ("a" + "b") AS "c"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST('2022-04-13 09:15:00' AS TIMESTAMP)
          AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMP)
    )
)
GROUP BY
  tile_index,
  "biz_id"
ORDER BY
  tile_index
    )
    ),
REQUEST_TABLE AS (SELECT
  CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
  'C1' AS "CUSTOMER_ID"),
REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ."CUSTOMER_ID",
        T.value::INTEGER AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "CUSTOMER_ID" FROM REQUEST_TABLE
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
        REQ."CUSTOMER_ID",
        T.value::INTEGER AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "CUSTOMER_ID" FROM REQUEST_TABLE
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
REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ."BUSINESS_ID",
        T.value::INTEGER AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "BUSINESS_ID" FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                604800,
                3600,
                900,
                1800
            )
        )
    ) T
),
_FB_AGGREGATED AS (SELECT
  REQ."POINT_IN_TIME",
  REQ."CUSTOMER_ID",
  "T0"."agg_w7200_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f" AS "agg_w7200_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f",
  "T1"."agg_w172800_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f" AS "agg_w172800_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f",
  "T2"."agg_w604800_sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1" AS "agg_w604800_sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1"
FROM REQUEST_TABLE AS REQ
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID",
      SUM(sum_value) / SUM(count_value) AS "agg_w7200_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f"
    FROM REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID AS REQ
    INNER JOIN avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID"
) AS T0
  ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME
  AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID",
      SUM(sum_value) / SUM(count_value) AS "agg_w172800_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f"
    FROM REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID AS REQ
    INNER JOIN avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID"
) AS T1
  ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME
  AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."BUSINESS_ID",
      SUM(value) AS "agg_w604800_sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1"
    FROM REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID AS REQ
    INNER JOIN sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."BUSINESS_ID" = TILE."biz_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."BUSINESS_ID"
) AS T2
  ON REQ.POINT_IN_TIME = T2.POINT_IN_TIME
  AND REQ."BUSINESS_ID" = T2."BUSINESS_ID")
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  ("agg_w7200_avg_f3600_m1800_b900_53307fe1790a553cf1ca703e44b92619ad86dc8f" / NULLIF("agg_w604800_sum_f3600_m1800_b900_1a10fa8ac42309f34d1cf11eb15ed0e65e120de1", 0)) AS "Unnamed"
FROM _FB_AGGREGATED AS AGG
