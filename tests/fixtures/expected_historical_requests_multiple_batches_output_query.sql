CREATE TABLE "__TEMP_0" AS
WITH "REQUEST_TABLE_order_id" AS (
  SELECT DISTINCT
    "order_id"
  FROM REQUEST_TABLE
), "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "MEMBERSHIP_STATUS"
  FROM REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_order_id_item_count_None_order_id_None_input_3" AS "_fb_internal_order_id_item_count_None_order_id_None_input_3",
    "T1"."_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4" AS "_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."order_id" AS "order_id",
      COUNT(*) AS "_fb_internal_order_id_item_count_None_order_id_None_input_3"
    FROM "REQUEST_TABLE_order_id" AS REQ
    INNER JOIN (
      SELECT
        "order_id" AS "order_id",
        "item_id" AS "item_id",
        "item_name" AS "item_name",
        "item_type" AS "item_type"
      FROM "db"."public"."item_table"
    ) AS ITEM
      ON REQ."order_id" = ITEM."order_id"
    GROUP BY
      REQ."order_id"
  ) AS T0
    ON REQ."order_id" = T0."order_id"
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS" AS "MEMBERSHIP_STATUS",
      COUNT(*) AS "_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4"
    FROM "REQUEST_TABLE_POINT_IN_TIME_MEMBERSHIP_STATUS" AS REQ
    INNER JOIN (
      SELECT
        *,
        LEAD("effective_ts") OVER (PARTITION BY "cust_id" ORDER BY "effective_ts") AS "__FB_END_TS"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
      )
    ) AS SCD
      ON REQ."MEMBERSHIP_STATUS" = SCD."membership_status"
      AND (
        SCD."effective_ts" <= REQ."POINT_IN_TIME"
        AND (
          SCD."__FB_END_TS" > REQ."POINT_IN_TIME" OR SCD."__FB_END_TS" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."MEMBERSHIP_STATUS"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME"
    AND REQ."MEMBERSHIP_STATUS" = T1."MEMBERSHIP_STATUS"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4" AS BIGINT) AS "asat_feature",
  CAST("_fb_internal_order_id_item_count_None_order_id_None_input_3" AS BIGINT) AS "order_size"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_1" AS
WITH _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R.value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78 AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_KEY_COL_1",
        "__FB_LAST_TS",
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          "__FB_KEY_COL_1",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TABLE_ROW_INDEX",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(FLOOR((
              DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
            ) / 3600) AS BIGINT) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            "BUSINESS_ID" AS "__FB_KEY_COL_1",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            0 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID"
            FROM REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            "INDEX" AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "biz_id" AS "__FB_KEY_COL_1",
            "INDEX" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID"
          FROM TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS R
      ON L."__FB_LAST_TS" = R."INDEX"
      AND L."__FB_KEY_COL_0" = R."cust_id"
      AND L."__FB_KEY_COL_1" = R."biz_id"
  ) AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID",
      ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2") AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
      ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2") AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
    FROM (
      SELECT
        "cust_id" AS "CUSTOMER_ID",
        "cust_value_1" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
        "cust_value_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
      FROM (
        SELECT
          "cust_id" AS "cust_id",
          "cust_value_1" AS "cust_value_1",
          "cust_value_2" AS "cust_value_2"
        FROM "db"."public"."dimension_table"
      )
    )
    GROUP BY
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS DOUBLE) AS "a_latest_value",
  CAST((
    "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" + "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
  ) AS DOUBLE) AS "MY FEATURE"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_2" AS
WITH "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 2160 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0" AS "_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0"
  FROM (
    SELECT
      L."__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      R."membership_status" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "__FB_TABLE_ROW_INDEX",
        "POINT_IN_TIME",
        "CUSTOMER_ID"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "__FB_TABLE_ROW_INDEX",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "__FB_TABLE_ROW_INDEX" AS "__FB_TABLE_ROW_INDEX",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID"
          FROM (
            SELECT
              REQ."__FB_TABLE_ROW_INDEX",
              REQ."POINT_IN_TIME",
              REQ."CUSTOMER_ID"
            FROM REQUEST_TABLE AS REQ
          )
          UNION ALL
          SELECT
            CONVERT_TIMEZONE('UTC', "event_timestamp") AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "__FB_TABLE_ROW_INDEX",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID"
          FROM (
            SELECT
              "effective_ts" AS "effective_ts",
              "cust_id" AS "cust_id",
              "membership_status" AS "membership_status"
            FROM "db"."public"."customer_profile_table"
            WHERE
              "event_timestamp" IS NOT NULL
          )
        )
      )
      WHERE
        "__FB_EFFECTIVE_TS_COL" IS NULL
    ) AS L
    LEFT JOIN (
      SELECT
        ANY_VALUE("effective_ts") AS "effective_ts",
        "cust_id",
        ANY_VALUE("membership_status") AS "membership_status"
      FROM (
        SELECT
          "effective_ts" AS "effective_ts",
          "cust_id" AS "cust_id",
          "membership_status" AS "membership_status"
        FROM "db"."public"."customer_profile_table"
        WHERE
          "event_timestamp" IS NOT NULL
      )
      GROUP BY
        "event_timestamp",
        "cust_id"
    ) AS R
      ON L."__FB_LAST_TS" = R."event_timestamp" AND L."__FB_KEY_COL_0" = R."cust_id"
  ) AS REQ
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE(value_latest_3f3abc1633f14c3f5e885311459179516d5622a0) OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.value_latest_3f3abc1633f14c3f5e885311459179516d5622a0
        FROM "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2160) = FLOOR(TILE.INDEX / 2160)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.value_latest_3f3abc1633f14c3f5e885311459179516d5622a0
        FROM "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
        INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2160) - 1 = FLOOR(TILE.INDEX / 2160)
          AND REQ."CUSTOMER_ID" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4" AS "Current Membership Status",
  CAST("_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0" AS DOUBLE) AS "a_latest_value_past_90d"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "__TEMP_3" AS
WITH "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
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
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T1"."_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) / SUM(count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) / SUM(count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_CUSTOMER_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."CUSTOMER_ID" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "CUSTOMER_ID"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS DOUBLE) AS "a_2h_average",
  CAST("_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS DOUBLE) AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "SOME_HISTORICAL_FEATURE_TABLE" AS
SELECT
  REQ."POINT_IN_TIME",
  REQ."CUSTOMER_ID",
  T3."a_2h_average",
  T3."a_48h_average",
  T0."order_size",
  T1."MY FEATURE",
  T2."Current Membership Status",
  T2."a_latest_value_past_90d",
  T1."a_latest_value",
  T0."asat_feature"
FROM "REQUEST_TABLE" AS REQ
LEFT JOIN "__TEMP_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX"
LEFT JOIN "__TEMP_1" AS T1
  ON REQ."__FB_TABLE_ROW_INDEX" = T1."__FB_TABLE_ROW_INDEX"
LEFT JOIN "__TEMP_2" AS T2
  ON REQ."__FB_TABLE_ROW_INDEX" = T2."__FB_TABLE_ROW_INDEX"
LEFT JOIN "__TEMP_3" AS T3
  ON REQ."__FB_TABLE_ROW_INDEX" = T3."__FB_TABLE_ROW_INDEX"
