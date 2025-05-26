CREATE TABLE "ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C" AS
SELECT * FROM (SELECT "cust_id",
                      CAST("AGGREGATION_RESULT_NAME" AS STRING) AS "AGGREGATION_RESULT_NAME",
                      CAST("VALUE" AS FLOAT) AS "VALUE",
                      CAST(0 AS INT) AS "VERSION",
                      CAST('2022-05-15 10:00:05' AS TIMESTAMP) AS UPDATED_AT
                    FROM (SELECT
  "cust_id",
  CAST('_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9' AS VARCHAR) AS "AGGREGATION_RESULT_NAME",
  "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9" AS "VALUE"
FROM (
  WITH REQUEST_TABLE AS (
    SELECT DISTINCT
      CAST('2022-05-15 09:15:00' AS TIMESTAMP) AS POINT_IN_TIME,
      "cust_id" AS "cust_id"
    FROM __FB_DEPLOYED_TILE_TABLE_000000000000000000000000
    WHERE
      INDEX >= CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 09:15:00' AS TIMESTAMP)) - 900
        ) / 3600
      ) AS BIGINT) - 2
      AND INDEX < CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 09:15:00' AS TIMESTAMP)) - 900
        ) / 3600
      ) AS BIGINT)
  ), "REQUEST_TABLE_W7200_F3600_BS1800_M900_cust_id" AS (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) AS BIGINT) - 2 AS __FB_FIRST_TILE_INDEX
    FROM (
      SELECT DISTINCT
        "POINT_IN_TIME",
        "cust_id"
      FROM REQUEST_TABLE
    )
  ), _FB_AGGREGATED AS (
    SELECT
      REQ."POINT_IN_TIME",
      REQ."cust_id",
      "T0"."_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9" AS "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "POINT_IN_TIME",
        "cust_id",
        SUM(value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9) AS "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
        FROM "REQUEST_TABLE_W7200_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 2) = FLOOR(TILE.INDEX / 2)
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
        FROM "REQUEST_TABLE_W7200_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 AS TILE
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
  )
  SELECT
    AGG."POINT_IN_TIME",
    AGG."cust_id",
    "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
  FROM _FB_AGGREGATED AS AGG
))
);
