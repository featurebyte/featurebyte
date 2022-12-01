WITH fake_transactions_table_f3600_m1800_b900_6df75fa33c5905ea927c25219b178c8848027e3c AS (
  SELECT
    sum_3c43724b30c443ac5ac9049578e0e6061173bb69.INDEX,
    sum_3c43724b30c443ac5ac9049578e0e6061173bb69."biz_id",
    value_sum_3c43724b30c443ac5ac9049578e0e6061173bb69
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-04-13 09:15:00' AS TIMESTAMP)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "biz_id",
        SUM("a") AS value_sum_3c43724b30c443ac5ac9049578e0e6061173bb69
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-13 09:15:00' AS TIMESTAMP))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            *
          FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "a" AS "a",
              "b" AS "b",
              (
                "a" + "b"
              ) AS "c"
            FROM "db"."public"."event_table"
          )
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
  ) AS sum_3c43724b30c443ac5ac9049578e0e6061173bb69
), fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d AS (
  SELECT
    avg_edade899e2fad6f29dfd3cad353742ff31964e12.INDEX,
    avg_edade899e2fad6f29dfd3cad353742ff31964e12."cust_id",
    sum_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12,
    count_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        TO_TIMESTAMP(
          DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP)) + tile_index * 3600
        ) AS __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        SUM("a") AS sum_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12,
        COUNT("a") AS count_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12
      FROM (
        SELECT
          *,
          FLOOR(
            (
              DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-04-18 09:15:00' AS TIMESTAMP))
            ) / 3600
          ) AS tile_index
        FROM (
          SELECT
            *
          FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "a" AS "a",
              "b" AS "b",
              (
                "a" + "b"
              ) AS "c"
            FROM "db"."public"."event_table"
          )
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
  ) AS avg_edade899e2fad6f29dfd3cad353742ff31964e12
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    POINT_IN_TIME,
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      POINT_IN_TIME,
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    POINT_IN_TIME,
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      POINT_IN_TIME,
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID" AS (
  SELECT
    POINT_IN_TIME,
    "BUSINESS_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, POINT_IN_TIME) - 1800
    ) / 3600) - 168 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      POINT_IN_TIME,
      "BUSINESS_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."agg_w7200_avg_edade899e2fad6f29dfd3cad353742ff31964e12" AS "agg_w7200_avg_edade899e2fad6f29dfd3cad353742ff31964e12",
    "T1"."agg_w172800_avg_edade899e2fad6f29dfd3cad353742ff31964e12" AS "agg_w172800_avg_edade899e2fad6f29dfd3cad353742ff31964e12",
    "T2"."agg_w604800_sum_3c43724b30c443ac5ac9049578e0e6061173bb69" AS "agg_w604800_sum_3c43724b30c443ac5ac9049578e0e6061173bb69"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12) / SUM(count_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12) AS "agg_w7200_avg_edade899e2fad6f29dfd3cad353742ff31964e12"
    FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID"
  ) AS T0
    ON REQ.POINT_IN_TIME = T0.POINT_IN_TIME AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12) / SUM(count_value_avg_edade899e2fad6f29dfd3cad353742ff31964e12) AS "agg_w172800_avg_edade899e2fad6f29dfd3cad353742ff31964e12"
    FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."CUSTOMER_ID"
  ) AS T1
    ON REQ.POINT_IN_TIME = T1.POINT_IN_TIME AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ.POINT_IN_TIME,
      REQ."BUSINESS_ID",
      SUM(value_sum_3c43724b30c443ac5ac9049578e0e6061173bb69) AS "agg_w604800_sum_3c43724b30c443ac5ac9049578e0e6061173bb69"
    FROM "REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID" AS REQ
    INNER JOIN fake_transactions_table_f3600_m1800_b900_6df75fa33c5905ea927c25219b178c8848027e3c AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) = FLOOR(TILE.INDEX / 168)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) - 1 = FLOOR(TILE.INDEX / 168)
      )
      AND REQ."BUSINESS_ID" = TILE."biz_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ.POINT_IN_TIME,
      REQ."BUSINESS_ID"
  ) AS T2
    ON REQ.POINT_IN_TIME = T2.POINT_IN_TIME AND REQ."BUSINESS_ID" = T2."BUSINESS_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  (
    "agg_w7200_avg_edade899e2fad6f29dfd3cad353742ff31964e12" / NULLIF("agg_w604800_sum_3c43724b30c443ac5ac9049578e0e6061173bb69", 0)
  ) AS "Unnamed"
FROM _FB_AGGREGATED AS AGG
