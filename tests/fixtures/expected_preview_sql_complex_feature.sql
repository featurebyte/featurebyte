WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624 AS (
  SELECT
    sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8.INDEX,
    sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8."biz_id",
    value_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8
  FROM (
    SELECT
      index,
      "biz_id",
      SUM("a") AS value_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM (
        WITH __FB_ENTITY_TABLE_NAME AS (
          (
            SELECT
              "BUSINESS_ID" AS "biz_id",
              TO_TIMESTAMP(
                FLOOR((
                  DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                ) / 3600) * 3600 + 1800 - 900
              ) AS "__FB_ENTITY_TABLE_END_DATE",
              DATEADD(
                microsecond,
                (
                  168 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
                ) * -1,
                TO_TIMESTAMP(
                  FLOOR((
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                  ) / 3600) * 3600 + 1800 - 900
                )
              ) AS "__FB_ENTITY_TABLE_START_DATE"
            FROM "REQUEST_TABLE"
            GROUP BY
              "BUSINESS_ID"
          )
        )
        SELECT
          R.*
        FROM __FB_ENTITY_TABLE_NAME
        INNER JOIN (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "a" AS "a",
            "b" AS "b",
            (
              "a" + "b"
            ) AS "c"
          FROM "db"."public"."event_table"
        ) AS R
          ON R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
          AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
          AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
      )
    )
    GROUP BY
      index,
      "biz_id"
  ) AS sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb.INDEX,
    avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb."cust_id",
    sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
    count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
  FROM (
    SELECT
      index,
      "cust_id",
      SUM("a") AS sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
      COUNT("a") AS count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM (
        WITH __FB_ENTITY_TABLE_NAME AS (
          (
            SELECT
              "CUSTOMER_ID" AS "cust_id",
              TO_TIMESTAMP(
                FLOOR((
                  DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                ) / 3600) * 3600 + 1800 - 900
              ) AS "__FB_ENTITY_TABLE_END_DATE",
              DATEADD(
                microsecond,
                (
                  48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
                ) * -1,
                TO_TIMESTAMP(
                  FLOOR((
                    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 1800
                  ) / 3600) * 3600 + 1800 - 900
                )
              ) AS "__FB_ENTITY_TABLE_START_DATE"
            FROM "REQUEST_TABLE"
            GROUP BY
              "CUSTOMER_ID"
          )
        )
        SELECT
          R.*
        FROM __FB_ENTITY_TABLE_NAME
        INNER JOIN (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "a" AS "a",
            "b" AS "b",
            (
              "a" + "b"
            ) AS "c"
          FROM "db"."public"."event_table"
        ) AS R
          ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
          AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
          AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
      )
    )
    GROUP BY
      index,
      "cust_id"
  ) AS avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
), "REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "BUSINESS_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 168 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "BUSINESS_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T1"."_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb",
    "T2"."_fb_internal_window_w604800_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8" AS "_fb_internal_window_w604800_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
      SUM(sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) / SUM(count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb) AS "_fb_internal_window_w172800_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
        TILE.count_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb,
        TILE.sum_value_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb
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
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "BUSINESS_ID",
      SUM(value_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8) AS "_fb_internal_window_w604800_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."BUSINESS_ID",
        TILE.INDEX,
        TILE.value_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8
      FROM "REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) = FLOOR(TILE.INDEX / 168)
        AND REQ."BUSINESS_ID" = TILE."biz_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."BUSINESS_ID",
        TILE.INDEX,
        TILE.value_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8
      FROM "REQUEST_TABLE_W604800_F3600_BS900_M1800_BUSINESS_ID" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) - 1 = FLOOR(TILE.INDEX / 168)
        AND REQ."BUSINESS_ID" = TILE."biz_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "BUSINESS_ID"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME" AND REQ."BUSINESS_ID" = T2."BUSINESS_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  (
    "_fb_internal_window_w7200_avg_30d0e03bfdc9aa70e3001f8c32a5f82e6f793cbb" / NULLIF("_fb_internal_window_w604800_sum_ea3e51f28222785a9bc856e4f09a8ce4642bc6c8", 0)
  ) AS "a_2h_avg_by_user_div_7d_by_biz"
FROM _FB_AGGREGATED AS AGG
