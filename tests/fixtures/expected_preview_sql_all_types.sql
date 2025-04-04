WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_13c45b8622761dd28afb4640ac3ed355d57d789f.INDEX,
    avg_13c45b8622761dd28afb4640ac3ed355d57d789f."cust_id",
    sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
    count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
    value_latest_3f3abc1633f14c3f5e885311459179516d5622a0
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            2160 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      SUM("input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f") AS sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
      COUNT("input_col_avg_13c45b8622761dd28afb4640ac3ed355d57d789f") AS count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id"
  ) AS avg_13c45b8622761dd28afb4640ac3ed355d57d789f
  RIGHT JOIN (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            2160 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "input_col_latest_3f3abc1633f14c3f5e885311459179516d5622a0"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      value_latest_3f3abc1633f14c3f5e885311459179516d5622a0
    FROM (
      SELECT
        index,
        "cust_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("input_col_latest_3f3abc1633f14c3f5e885311459179516d5622a0") OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_3f3abc1633f14c3f5e885311459179516d5622a0
      FROM (
        SELECT
          *,
          F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
        FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_3f3abc1633f14c3f5e885311459179516d5622a0
    ON avg_13c45b8622761dd28afb4640ac3ed355d57d789f.INDEX = latest_3f3abc1633f14c3f5e885311459179516d5622a0.INDEX
    AND avg_13c45b8622761dd28afb4640ac3ed355d57d789f."cust_id" = latest_3f3abc1633f14c3f5e885311459179516d5622a0."cust_id"
), TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E AS (
  SELECT
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78.INDEX,
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78."cust_id",
    latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78."biz_id",
    value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "CUSTOMER_ID" AS "cust_id",
        "BUSINESS_ID" AS "biz_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
        ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            (
              1800 - 900
            ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ),
          CAST('1970-01-01' AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "CUSTOMER_ID",
        "BUSINESS_ID"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "biz_id" AS "biz_id",
          "a" AS "input_col_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      "biz_id",
      value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
    FROM (
      SELECT
        index,
        "cust_id",
        "biz_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("input_col_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78") OVER (PARTITION BY index, "cust_id", "biz_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
      FROM (
        SELECT
          *,
          F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
        FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_CUSTOMER_ID" AS (
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
), "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS (
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
), "REQUEST_TABLE_order_id" AS (
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
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78",
    REQ."_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2" AS "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2",
    "T1"."_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T2"."_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T3"."_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0" AS "_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0",
    "T4"."_fb_internal_order_id_item_count_None_order_id_None_input_3" AS "_fb_internal_order_id_item_count_None_order_id_None_input_3",
    "T5"."_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4" AS "_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4"
  FROM (
    SELECT
      L."POINT_IN_TIME" AS "POINT_IN_TIME",
      L."CUSTOMER_ID" AS "CUSTOMER_ID",
      L."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78",
      R."membership_status" AS "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4"
    FROM (
      SELECT
        "__FB_KEY_COL_0",
        "__FB_LAST_TS",
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
      FROM (
        SELECT
          "__FB_KEY_COL_0",
          LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
          "POINT_IN_TIME",
          "CUSTOMER_ID",
          "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78",
          "__FB_EFFECTIVE_TS_COL"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "POINT_IN_TIME") AS TIMESTAMP) AS "__FB_TS_COL",
            "CUSTOMER_ID" AS "__FB_KEY_COL_0",
            NULL AS "__FB_EFFECTIVE_TS_COL",
            2 AS "__FB_TS_TIE_BREAKER_COL",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "CUSTOMER_ID" AS "CUSTOMER_ID",
            "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
          FROM (
            SELECT
              REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
              REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
              REQ."_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
            FROM (
              SELECT
                L."POINT_IN_TIME" AS "POINT_IN_TIME",
                L."CUSTOMER_ID" AS "CUSTOMER_ID",
                R.value_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78 AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
              FROM (
                SELECT
                  "__FB_KEY_COL_0",
                  "__FB_KEY_COL_1",
                  "__FB_LAST_TS",
                  "POINT_IN_TIME",
                  "CUSTOMER_ID"
                FROM (
                  SELECT
                    "__FB_KEY_COL_0",
                    "__FB_KEY_COL_1",
                    LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0", "__FB_KEY_COL_1" ORDER BY "__FB_TS_COL" NULLS FIRST, "__FB_TS_TIE_BREAKER_COL") AS "__FB_LAST_TS",
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
                      "INDEX" AS "__FB_TS_COL",
                      "cust_id" AS "__FB_KEY_COL_0",
                      "biz_id" AS "__FB_KEY_COL_1",
                      "INDEX" AS "__FB_EFFECTIVE_TS_COL",
                      1 AS "__FB_TS_TIE_BREAKER_COL",
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
          )
          UNION ALL
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
            "cust_id" AS "__FB_KEY_COL_0",
            "event_timestamp" AS "__FB_EFFECTIVE_TS_COL",
            1 AS "__FB_TS_TIE_BREAKER_COL",
            NULL AS "POINT_IN_TIME",
            NULL AS "CUSTOMER_ID",
            NULL AS "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
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
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
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
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
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
  ) AS T3
    ON REQ."POINT_IN_TIME" = T3."POINT_IN_TIME" AND REQ."CUSTOMER_ID" = T3."CUSTOMER_ID"
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
  ) AS T4
    ON REQ."order_id" = T4."order_id"
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
  ) AS T5
    ON REQ."POINT_IN_TIME" = T5."POINT_IN_TIME"
    AND REQ."MEMBERSHIP_STATUS" = T5."MEMBERSHIP_STATUS"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS DOUBLE) AS "a_2h_average",
  CAST("_fb_internal_CUSTOMER_ID_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS DOUBLE) AS "a_48h_average",
  CAST("_fb_internal_order_id_item_count_None_order_id_None_input_3" AS BIGINT) AS "order_size",
  CAST((
    "_fb_internal_CUSTOMER_ID_lookup_cust_value_1_input_2" + "_fb_internal_CUSTOMER_ID_lookup_cust_value_2_input_2"
  ) AS DOUBLE) AS "MY FEATURE",
  "_fb_internal_CUSTOMER_ID_lookup_membership_status_input_4" AS "Current Membership Status",
  CAST("_fb_internal_CUSTOMER_ID_window_w7776000_latest_3f3abc1633f14c3f5e885311459179516d5622a0" AS DOUBLE) AS "a_latest_value_past_90d",
  CAST("_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78" AS DOUBLE) AS "a_latest_value",
  CAST("_fb_internal_MEMBERSHIP_STATUS_as_at_count_None_membership_status_None_input_4" AS BIGINT) AS "asat_feature"
FROM _FB_AGGREGATED AS AGG
