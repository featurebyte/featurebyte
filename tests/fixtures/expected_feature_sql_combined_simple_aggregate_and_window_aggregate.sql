WITH "REQUEST_TABLE_W604800_F360_BS90_M180_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 180
    ) / 360) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 180
    ) / 360) AS BIGINT) - 1680 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE
  )
), "REQUEST_TABLE_transaction_id" AS (
  SELECT DISTINCT
    "transaction_id"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    "T0"."_fb_internal_cust_id_window_w604800_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d" AS "_fb_internal_cust_id_window_w604800_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d",
    "T1"."_fb_internal_transaction_id_item_count_None_event_id_col_None_join_1" AS "_fb_internal_transaction_id_item_count_None_event_id_col_None_join_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d) AS "_fb_internal_cust_id_window_w604800_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d
      FROM "REQUEST_TABLE_W604800_F360_BS90_M180_cust_id" AS REQ
      INNER JOIN TILE_SUM_B0B3ECC1A6233735C72F7224F1B7CAAA4E2AB60D AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1680) = FLOOR(TILE.INDEX / 1680)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d
      FROM "REQUEST_TABLE_W604800_F360_BS90_M180_cust_id" AS REQ
      INNER JOIN TILE_SUM_B0B3ECC1A6233735C72F7224F1B7CAAA4E2AB60D AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 1680) - 1 = FLOOR(TILE.INDEX / 1680)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
  LEFT JOIN (
    SELECT
      REQ."transaction_id" AS "transaction_id",
      COUNT(*) AS "_fb_internal_transaction_id_item_count_None_event_id_col_None_join_1"
    FROM "REQUEST_TABLE_transaction_id" AS REQ
    INNER JOIN (
      SELECT
        L."event_id_col" AS "event_id_col",
        L."item_id_col" AS "item_id_col",
        L."item_type" AS "item_type",
        L."item_amount" AS "item_amount",
        L."created_at" AS "created_at",
        L."event_timestamp" AS "event_timestamp",
        R."event_timestamp" AS "event_timestamp_event",
        R."cust_id" AS "cust_id_event"
      FROM (
        SELECT
          "event_id_col" AS "event_id_col",
          "item_id_col" AS "item_id_col",
          "item_type" AS "item_type",
          "item_amount" AS "item_amount",
          "created_at" AS "created_at",
          "event_timestamp" AS "event_timestamp"
        FROM "sf_database"."sf_schema"."items_table"
      ) AS L
      INNER JOIN (
        SELECT
          "col_int",
          ANY_VALUE("col_float") AS "col_float",
          ANY_VALUE("col_char") AS "col_char",
          ANY_VALUE("col_text") AS "col_text",
          ANY_VALUE("col_binary") AS "col_binary",
          ANY_VALUE("col_boolean") AS "col_boolean",
          ANY_VALUE("event_timestamp") AS "event_timestamp",
          ANY_VALUE("cust_id") AS "cust_id"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        )
        GROUP BY
          "col_int"
      ) AS R
        ON L."event_id_col" = R."col_int"
    ) AS ITEM
      ON REQ."transaction_id" = ITEM."event_id_col"
    GROUP BY
      REQ."transaction_id"
  ) AS T1
    ON REQ."transaction_id" = T1."transaction_id"
)
SELECT
  CAST((
    "_fb_internal_cust_id_window_w604800_sum_b0b3ecc1a6233735c72f7224f1b7caaa4e2ab60d" + "_fb_internal_transaction_id_item_count_None_event_id_col_None_join_1"
  ) AS BIGINT) AS "combined_feature"
FROM _FB_AGGREGATED AS AGG
