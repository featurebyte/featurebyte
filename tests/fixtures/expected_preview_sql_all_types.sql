WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8.INDEX,
    avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8."cust_id",
    sum_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8,
    count_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8
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
        SUM("a") AS sum_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8,
        COUNT("a") AS count_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8
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
  ) AS avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 2 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 48 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."membership_status_a18d6f89f8538bdb" AS "membership_status_a18d6f89f8538bdb",
    "T0"."cust_value_1_9b8bee3acf7d5bc7" AS "cust_value_1_9b8bee3acf7d5bc7",
    "T0"."cust_value_2_9b8bee3acf7d5bc7" AS "cust_value_2_9b8bee3acf7d5bc7",
    "T1"."agg_w7200_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8" AS "agg_w7200_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8",
    "T2"."agg_w172800_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8" AS "agg_w172800_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8",
    "T3"."order_size" AS "order_size"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R."membership_status" AS "membership_status_a18d6f89f8538bdb"
    FROM (
      SELECT
        "__FB_KEY_COL",
        "__FB_LAST_TS",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL" ORDER BY "__FB_TS_COL" NULLS LAST, "__FB_EFFECTIVE_TS_COL" NULLS LAST) AS "__FB_LAST_TS",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            "POINT_IN_TIME" AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID"
            FROM REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            "event_timestamp" AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        "effective_ts" AS "effective_ts",
        "cust_id" AS "cust_id",
        "membership_status" AS "membership_status"
      FROM "db"."public"."customer_profile_table"
    ) AS R
      ON L."__FB_LAST_TS" = R."event_timestamp" AND L."__FB_KEY_COL" = R."cust_id"
  ) AS REQ
  LEFT JOIN (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "cust_value_1" AS "cust_value_1_9b8bee3acf7d5bc7",
      "cust_value_2" AS "cust_value_2_9b8bee3acf7d5bc7"
    FROM (
      SELECT
        "cust_id" AS "cust_id",
        "cust_value_1" AS "cust_value_1",
        "cust_value_2" AS "cust_value_2"
      FROM "db"."public"."dimension_table"
    )
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8) / SUM(count_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8) AS "agg_w7200_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8"
    FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
      SUM(sum_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8) / SUM(count_value_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8) AS "agg_w172800_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8"
    FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
    INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
      ON (
        FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        OR FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
      )
      AND REQ."CUSTOMER_ID" = TILE."cust_id"
    WHERE
      TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
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
  ) AS T3
    ON REQ."order_id" = T3."order_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "agg_w7200_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8" AS "a_2h_average",
  "agg_w172800_avg_33d7045ac1aea1e0a20f32ca16f997f220f5cbc8" AS "a_48h_average",
  "order_size" AS "order_size",
  (
    "cust_value_1_9b8bee3acf7d5bc7" + "cust_value_2_9b8bee3acf7d5bc7"
  ) AS "MY FEATURE",
  "membership_status_a18d6f89f8538bdb" AS "Current Membership Status"
FROM _FB_AGGREGATED AS AGG
