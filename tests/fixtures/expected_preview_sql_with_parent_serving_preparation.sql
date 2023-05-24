WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac.INDEX,
    avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac."cust_id",
    sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
    count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
  FROM (
    SELECT
      index,
      "cust_id",
      SUM("a") AS sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
      COUNT("a") AS count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
    FROM (
      SELECT
        *,
        F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "ts"), 1800, 900, 60) AS index
      FROM (
        SELECT
          *
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            CASE WHEN (
              "a" IS NULL
            ) THEN 0 ELSE "a" END AS "a",
            "b" AS "b"
          FROM "db"."public"."event_table"
        )
        WHERE
          "ts" >= CAST('2022-04-18 09:15:00' AS TIMESTAMPNTZ)
          AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
      )
    )
    GROUP BY
      index,
      "cust_id"
  ) AS avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
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
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
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
    ) AS T0
      ON REQ."COL_TEXT" = T0."COL_TEXT"
  ) AS REQ
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
    FROM JOINED_PARENTS_REQUEST_TABLE
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
    FROM JOINED_PARENTS_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    REQ."COL_INT",
    "T0"."_fb_internal_window_w7200_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac" AS "_fb_internal_window_w7200_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac",
    "T1"."_fb_internal_window_w172800_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac" AS "_fb_internal_window_w172800_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac"
  FROM JOINED_PARENTS_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "CUSTOMER_ID",
      SUM(sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac) / SUM(count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac) AS "_fb_internal_window_w7200_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
        TILE.sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
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
        TILE.count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
        TILE.sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
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
      SUM(sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac) / SUM(count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac) AS "_fb_internal_window_w172800_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."CUSTOMER_ID",
        TILE.INDEX,
        TILE.count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
        TILE.sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
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
        TILE.count_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac,
        TILE.sum_value_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac
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
  "_fb_internal_window_w7200_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac" AS "a_2h_average",
  "_fb_internal_window_w172800_avg_d2afc651cc81ba20447f12d1bc06cf1aa00fe8ac" AS "a_48h_average"
FROM _FB_AGGREGATED AS AGG
