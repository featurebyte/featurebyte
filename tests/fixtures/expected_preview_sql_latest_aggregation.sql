WITH TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1.INDEX,
    latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1."cust_id",
    value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
  FROM (
    SELECT
      *,
      F_TIMESTAMP_TO_INDEX(__FB_TILE_START_DATE_COLUMN, 1800, 900, 60) AS "INDEX"
    FROM (
      SELECT
        __FB_TILE_START_DATE_COLUMN,
        "cust_id",
        value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
      FROM (
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          ROW_NUMBER() OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
          FIRST_VALUE("a") OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ))
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
                "b" AS "b"
              FROM "db"."public"."event_table"
            )
            WHERE
              "ts" >= CAST('2022-01-20 09:15:00' AS TIMESTAMPNTZ)
              AND "ts" < CAST('2022-04-20 09:15:00' AS TIMESTAMPNTZ)
          )
        )
      )
      WHERE
        "__FB_ROW_NUMBER" = 1
    )
  ) AS latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
), REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), "REQUEST_TABLE_W7776000_F3600_BS900_M1800_CUSTOMER_ID" AS (
  SELECT
    "POINT_IN_TIME",
    "CUSTOMER_ID",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) AS "__FB_LAST_TILE_INDEX",
    FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 1800
    ) / 3600) - 2160 AS "__FB_FIRST_TILE_INDEX"
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "CUSTOMER_ID"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_window_w7776000_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1" AS "_fb_internal_window_w7776000_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE(value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1) OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "_fb_internal_window_w7776000_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
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
          TILE.value_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1
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
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  "_fb_internal_window_w7776000_latest_b956ae318e1a8832a8c6193e7a8fff7b0824d2d1" AS "a_latest_value_past_90d"
FROM _FB_AGGREGATED AS AGG
