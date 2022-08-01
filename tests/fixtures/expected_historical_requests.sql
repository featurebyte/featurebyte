WITH REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ."cust_id",
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "cust_id" FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                1800,
                1800,
                600,
                300
            )
        )
    ) T
),
REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ."cust_id",
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "cust_id" FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                7200,
                1800,
                600,
                300
            )
        )
    ) T
),
REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id AS (
    SELECT
        REQ.POINT_IN_TIME,
        REQ."cust_id",
        T.value AS REQ_TILE_INDEX
    FROM (
        SELECT DISTINCT POINT_IN_TIME, "cust_id" FROM REQUEST_TABLE
    ) REQ,
    Table(
        Flatten(
            SELECT F_COMPUTE_TILE_INDICES(
                DATE_PART(epoch, REQ.POINT_IN_TIME),
                86400,
                1800,
                600,
                300
            )
        )
    ) T
),
_FB_AGGREGATED AS (SELECT
  REQ.*,
  "T0"."agg_w1800_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512" AS "agg_w1800_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
  "T1"."agg_w7200_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512" AS "agg_w7200_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
  "T2"."agg_w86400_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512" AS "agg_w86400_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"
FROM REQUEST_TABLE AS REQ
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."cust_id",
      SUM(value) AS "agg_w1800_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"
    FROM REQUEST_TABLE_W1800_F1800_BS600_M300_cust_id AS REQ
    INNER JOIN sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."cust_id" = TILE."cust_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."cust_id"
) AS T0
  ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME
  AND REQ."cust_id" = T0."cust_id"
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."cust_id",
      SUM(value) AS "agg_w7200_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"
    FROM REQUEST_TABLE_W7200_F1800_BS600_M300_cust_id AS REQ
    INNER JOIN sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."cust_id" = TILE."cust_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."cust_id"
) AS T1
  ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME
  AND REQ."cust_id" = T1."cust_id"
LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."cust_id",
      SUM(value) AS "agg_w86400_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512"
    FROM REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id AS REQ
    INNER JOIN sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512 AS TILE
      ON REQ.REQ_TILE_INDEX = TILE.INDEX
      AND REQ."cust_id" = TILE."cust_id"
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."cust_id"
) AS T2
  ON REQ.POINT_IN_TIME = T2.POINT_IN_TIME
  AND REQ."cust_id" = T2."cust_id")
SELECT
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  AGG."A",
  AGG."B",
  AGG."C",
  "agg_w86400_sum_f1800_m300_b600_afb4d56e30a685ee9128bfa58fe4ad76d32af512" AS "sum_1d"
FROM _FB_AGGREGATED AS AGG
