WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMPNTZ) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725 AS (
  SELECT
    latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73.INDEX,
    latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73."cust_id",
    value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
  FROM (
    SELECT
      index,
      "cust_id",
      value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
    FROM (
      SELECT
        index,
        "cust_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("a") OVER (PARTITION BY index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
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
                    2160 * 3600 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
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
              "b" AS "b"
            FROM "db"."public"."event_table"
          ) AS R
            ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
            AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
            AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
        )
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
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
    "T0"."_fb_internal_window_w7776000_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73" AS "_fb_internal_window_w7776000_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        "POINT_IN_TIME",
        "CUSTOMER_ID",
        ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE(value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73) OVER (PARTITION BY "POINT_IN_TIME", "CUSTOMER_ID" ORDER BY INDEX DESC NULLS LAST) AS "_fb_internal_window_w7776000_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."CUSTOMER_ID",
          TILE.INDEX,
          TILE.value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
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
          TILE.value_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73
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
  "_fb_internal_window_w7776000_latest_2a1145d57c972a1eace23efb905e5f1e25ba5e73" AS "a_latest_value_past_90d"
FROM _FB_AGGREGATED AS AGG
