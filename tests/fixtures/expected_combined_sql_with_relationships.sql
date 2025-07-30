WITH JOINED_PARENTS_REQUEST_TABLE AS (
  SELECT
    REQ."a" AS "a",
    REQ."b" AS "b",
    REQ."c" AS "c",
    REQ."COL_INT" AS "COL_INT",
    REQ."cust_id_100000000000000000000000" AS "cust_id_100000000000000000000000"
  FROM (
    SELECT
      REQ."a" AS "a",
      REQ."b" AS "b",
      REQ."c" AS "c",
      REQ."COL_INT" AS "COL_INT",
      "T0"."cust_id_100000000000000000000000" AS "cust_id_100000000000000000000000"
    FROM (
      SELECT
        REQ."a",
        REQ."b",
        REQ."c",
        "T0"."COL_INT" AS "COL_INT"
      FROM "REQUEST_TABLE" AS REQ
      LEFT JOIN (
        SELECT
          "COL_TEXT",
          ANY_VALUE("COL_INT") AS "COL_INT"
        FROM (
          SELECT
            "col_text" AS "COL_TEXT",
            "col_int" AS "COL_INT"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "created_at" AS "created_at",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."dimension_table"
          )
        )
        GROUP BY
          "COL_TEXT"
      ) AS T0
        ON REQ."COL_TEXT" = T0."COL_TEXT"
    ) AS REQ
    LEFT JOIN (
      SELECT
        "cust_id",
        ANY_VALUE("cust_id_100000000000000000000000") AS "cust_id_100000000000000000000000"
      FROM (
        SELECT
          "relation_cust_id" AS "cust_id",
          "relation_biz_id" AS "cust_id_100000000000000000000000"
        FROM (
          SELECT
            "relation_cust_id" AS "relation_cust_id",
            "relation_biz_id" AS "relation_biz_id"
          FROM "db"."public"."some_table_name"
        )
      )
      GROUP BY
        "cust_id"
    ) AS T0
      ON REQ."cust_id" = T0."cust_id"
  ) AS REQ
), "REQUEST_TABLE_W7200_F3600_BS900_M1800_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), "REQUEST_TABLE_W172800_F3600_BS900_M1800_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), "REQUEST_TABLE_W604800_F3600_BS900_M1800_cust_id_100000000000000000000000" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id_100000000000000000000000",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS BIGINT) - 168 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id_100000000000000000000000"
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."a",
    REQ."b",
    REQ."c",
    REQ."COL_INT",
    REQ."cust_id_100000000000000000000000",
    "T0"."_fb_internal_cust_id_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_cust_id_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T1"."_fb_internal_cust_id_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" AS "_fb_internal_cust_id_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
    "T2"."_fb_internal_cust_id_100000000000000000000000_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528" AS "_fb_internal_cust_id_100000000000000000000000_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528"
  FROM JOINED_PARENTS_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) / SUM(count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) AS "_fb_internal_cust_id_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_cust_id" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W7200_F3600_BS900_M1800_cust_id" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) - 1 = FLOOR(TILE.INDEX / 2)
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
      "POINT_IN_TIME",
      "cust_id",
      SUM(sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) / SUM(count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f) AS "_fb_internal_cust_id_window_w172800_avg_13c45b8622761dd28afb4640ac3ed355d57d789f"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_cust_id" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.count_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f,
        TILE.sum_value_avg_13c45b8622761dd28afb4640ac3ed355d57d789f
      FROM "REQUEST_TABLE_W172800_F3600_BS900_M1800_cust_id" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T1
    ON REQ."POINT_IN_TIME" = T1."POINT_IN_TIME" AND REQ."cust_id" = T1."cust_id"
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id_100000000000000000000000",
      SUM(value_sum_8c11e770ad5121aec588693662ac607b4fba0528) AS "_fb_internal_cust_id_100000000000000000000000_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id_100000000000000000000000",
        TILE.INDEX,
        TILE.value_sum_8c11e770ad5121aec588693662ac607b4fba0528
      FROM "REQUEST_TABLE_W604800_F3600_BS900_M1800_cust_id_100000000000000000000000" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) = FLOOR(TILE.INDEX / 168)
        AND REQ."cust_id_100000000000000000000000" = TILE."biz_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id_100000000000000000000000",
        TILE.INDEX,
        TILE.value_sum_8c11e770ad5121aec588693662ac607b4fba0528
      FROM "REQUEST_TABLE_W604800_F3600_BS900_M1800_cust_id_100000000000000000000000" AS REQ
      INNER JOIN TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 168) - 1 = FLOOR(TILE.INDEX / 168)
        AND REQ."cust_id_100000000000000000000000" = TILE."biz_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id_100000000000000000000000"
  ) AS T2
    ON REQ."POINT_IN_TIME" = T2."POINT_IN_TIME"
    AND REQ."cust_id_100000000000000000000000" = T2."cust_id_100000000000000000000000"
)
SELECT
  AGG."a",
  AGG."b",
  AGG."c",
  CAST((
    "_fb_internal_cust_id_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f" / NULLIF(
      "_fb_internal_cust_id_100000000000000000000000_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528",
      0
    )
  ) AS DOUBLE) AS "a_2h_avg_by_user_div_7d_by_biz"
FROM _FB_AGGREGATED AS AGG
