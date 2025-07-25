WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), JOINED_PARENTS_REQUEST_TABLE AS (
  SELECT
    REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
    REQ."CUSTOMER_ID" AS "CUSTOMER_ID",
    REQ."COL_INT" AS "COL_INT"
  FROM (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."CUSTOMER_ID",
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
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8.INDEX,
    avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8."cust_id",
    sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
    count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
            48 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 1800
          ) / 3600) * 3600 + 1800 - 900 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "JOINED_PARENTS_REQUEST_TABLE"
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
          CASE WHEN (
            "a" IS NULL
          ) THEN 0 ELSE "a" END AS "input_col_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8"
        FROM "db"."public"."event_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      SUM("input_col_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8") AS sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
      COUNT("input_col_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8") AS count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
      FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
    )
    GROUP BY
      index,
      "cust_id"
  ) AS avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
    FROM JOINED_PARENTS_REQUEST_TABLE
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
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    REQ."COL_INT",
    "T0"."_fb_internal_CUSTOMER_ID_window_w7200_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8" AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8",
    "T1"."_fb_internal_CUSTOMER_ID_window_w172800_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8" AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8"
  FROM JOINED_PARENTS_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8) / SUM(count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8) AS "_fb_internal_CUSTOMER_ID_window_w7200_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
        TILE.sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
        TILE.count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
        TILE.sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
      SUM(sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8) / SUM(count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8) AS "_fb_internal_CUSTOMER_ID_window_w172800_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
        TILE.sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
        TILE.count_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8,
        TILE.sum_value_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8
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
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_window_w7200_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8" AS DOUBLE) AS "a_2h_average",
  CAST("_fb_internal_CUSTOMER_ID_window_w172800_avg_8a71d7c7a86e5b0b808ed85f7e70ab6a3f4739a8" AS DOUBLE) AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
