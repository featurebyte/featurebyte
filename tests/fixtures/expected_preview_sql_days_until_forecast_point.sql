WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID",
    '2022-05-01' AS "FORECAST_POINT"
), TILE_LATEST_D9B2A8EBB02E7A6916AE36E9CC223759433C01E2 AS (
  SELECT
    latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2.INDEX,
    latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2."cust_id",
    value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        "cust_id" AS "cust_id",
        CAST(FLOOR((
          DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 300
        ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
        DATEADD(
          MICROSECOND,
          (
            4320 * 1800 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
          ) * -1,
          CAST(FLOOR((
            DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 300
          ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
        ) AS __FB_ENTITY_TABLE_START_DATE
      FROM "REQUEST_TABLE"
      GROUP BY
        "cust_id"
    ), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
      SELECT
        R.*
      FROM __FB_ENTITY_TABLE_NAME
      INNER JOIN (
        SELECT
          "event_timestamp" AS "event_timestamp",
          "cust_id" AS "cust_id",
          "event_timestamp" AS "input_col_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"
        FROM "sf_database"."sf_schema"."sf_table"
      ) AS R
        ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
        AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
        AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
    )
    SELECT
      index,
      "cust_id",
      value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
    FROM (
      SELECT
        index,
        "cust_id",
        ROW_NUMBER() OVER (PARTITION BY index, "cust_id" ORDER BY "event_timestamp" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE("input_col_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2") OVER (PARTITION BY index, "cust_id" ORDER BY "event_timestamp" DESC NULLS LAST) AS value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
      FROM (
        SELECT
          *,
          F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
        FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
), "REQUEST_TABLE_W7776000_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 4320 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    REQ."FORECAST_POINT",
    "T0"."_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2" AS "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        "POINT_IN_TIME",
        "cust_id",
        ROW_NUMBER() OVER (PARTITION BY "POINT_IN_TIME", "cust_id" ORDER BY INDEX DESC NULLS LAST) AS "__FB_ROW_NUMBER",
        FIRST_VALUE(value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2) OVER (PARTITION BY "POINT_IN_TIME", "cust_id" ORDER BY INDEX DESC NULLS LAST) AS "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
        FROM "REQUEST_TABLE_W7776000_F1800_BS600_M300_cust_id" AS REQ
        INNER JOIN TILE_LATEST_D9B2A8EBB02E7A6916AE36E9CC223759433C01E2 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4320) = FLOOR(TILE.INDEX / 4320)
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2
        FROM "REQUEST_TABLE_W7776000_F1800_BS600_M300_cust_id" AS REQ
        INNER JOIN TILE_LATEST_D9B2A8EBB02E7A6916AE36E9CC223759433C01E2 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4320) - 1 = FLOOR(TILE.INDEX / 4320)
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      )
    )
    WHERE
      "__FB_ROW_NUMBER" = 1
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  AGG."FORECAST_POINT",
  CAST((
    DATEDIFF(
      MICROSECOND,
      "_fb_internal_cust_id_window_w7776000_latest_d9b2a8ebb02e7a6916ae36e9cc223759433c01e2",
      CONVERT_TIMEZONE('America/New_York', 'UTC', CAST("FORECAST_POINT" AS TIMESTAMP))
    ) * CAST(1 AS BIGINT) / CAST(86400000000 AS BIGINT)
  ) AS DOUBLE) AS "days_until_forecast"
FROM _FB_AGGREGATED AS AGG
