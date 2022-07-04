WITH avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89 AS (
    SELECT *, F_TIMESTAMP_TO_INDEX(  TILE_START_DATE,  1800,  900,  60) AS "INDEX" FROM (
        SELECT
  TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP)) + tile_index * 3600) AS tile_start_date,
  "cust_id",
  SUM("a") AS sum_value,
  COUNT(*) AS count_value
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
          "a" + "b" AS "c"
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
REQUEST_TABLE AS (SELECT
  CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS POINT_IN_TIME,
  'C1' AS cust_id),
REQUEST_TABLE_W7200_F3600_BS900_M1800_cust_id AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ.cust_id,
        T.value AS REQ_TILE_INDEX
    FROM REQUEST_TABLE REQ,
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
REQUEST_TABLE_W172800_F3600_BS900_M1800_cust_id AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ.cust_id,
        T.value AS REQ_TILE_INDEX
    FROM REQUEST_TABLE REQ,
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
  "T0"."agg_w7200_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89" AS "agg_w7200_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89",
  "T1"."agg_w172800_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89" AS "agg_w172800_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89"
FROM REQUEST_TABLE AS REQ
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ.cust_id,
      SUM(sum_value) / SUM(count_value) AS "agg_w7200_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89"
    FROM REQUEST_TABLE_W7200_F3600_BS900_M1800_cust_id AS REQ
    INNER JOIN avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ.cust_id = TILE.cust_id
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ.cust_id
) AS T0
  ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME
  AND REQ.cust_id = T0.cust_id
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ.cust_id,
      SUM(sum_value) / SUM(count_value) AS "agg_w172800_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89"
    FROM REQUEST_TABLE_W172800_F3600_BS900_M1800_cust_id AS REQ
    INNER JOIN avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ.cust_id = TILE.cust_id
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ.cust_id
) AS T1
  ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME
  AND REQ.cust_id = T1.cust_id)
SELECT
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  "agg_w7200_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89" AS "a_2h_average",
  "agg_w172800_avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
