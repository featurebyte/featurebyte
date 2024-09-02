CREATE TABLE IF NOT EXISTS "TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9" AS
SELECT * FROM (SELECT * FROM (
            select
                index,
                "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9,
                current_timestamp() as created_at
            from (SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 900, 1800, 60) AS index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        "event_timestamp" >= CAST('2022-05-15T02:45:00.000000Z' AS TIMESTAMPNTZ)
        AND "event_timestamp" < CAST('2022-05-15T08:45:00.000000Z' AS TIMESTAMPNTZ)
    )
    WHERE
      "event_timestamp" >= CAST('2022-05-15T02:45:00.000000Z' AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST('2022-05-15T08:45:00.000000Z' AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id")
        ) LIMIT 0);


            merge into TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 a using (
            select
                index,
                "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9,
                current_timestamp() as created_at
            from (SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 900, 1800, 60) AS index
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "col_int" AS "col_int",
        "col_float" AS "col_float",
        "col_char" AS "col_char",
        "col_text" AS "col_text",
        "col_binary" AS "col_binary",
        "col_boolean" AS "col_boolean",
        "event_timestamp" AS "event_timestamp",
        "cust_id" AS "cust_id"
      FROM "sf_database"."sf_schema"."sf_table"
      WHERE
        "event_timestamp" >= CAST('2022-05-15T02:45:00.000000Z' AS TIMESTAMPNTZ)
        AND "event_timestamp" < CAST('2022-05-15T08:45:00.000000Z' AS TIMESTAMPNTZ)
    )
    WHERE
      "event_timestamp" >= CAST('2022-05-15T02:45:00.000000Z' AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST('2022-05-15T08:45:00.000000Z' AS TIMESTAMPNTZ)
  )
)
GROUP BY
  index,
  "cust_id")
        ) b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."cust_id", b."cust_id")
                when matched then
                    update set a.created_at = current_timestamp(), a.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 = b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
                when not matched then
                    insert (INDEX, "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, CREATED_AT)
                        values (b.INDEX, b."cust_id", b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, current_timestamp())
        ;

CREATE TABLE "ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C" AS
SELECT * FROM (SELECT "cust_id",
                      CAST("AGGREGATION_RESULT_NAME" AS STRING) AS "AGGREGATION_RESULT_NAME",
                      CAST("VALUE" AS FLOAT) AS "VALUE",
                      CAST(0 AS INT) AS "VERSION",
                      to_timestamp('2022-05-15 10:00:05') AS UPDATED_AT
                    FROM (SELECT
  "cust_id",
  CAST('_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400' AS VARCHAR) AS "AGGREGATION_RESULT_NAME",
  "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400" AS "VALUE"
FROM (
  WITH REQUEST_TABLE AS (
    SELECT DISTINCT
      CAST('2022-05-15 09:15:00' AS TIMESTAMPNTZ) AS POINT_IN_TIME,
      "cust_id" AS "cust_id"
    FROM TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9
    WHERE
      INDEX >= FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 09:15:00' AS TIMESTAMPNTZ)) - 900
        ) / 3600
      ) - 4 - 2
      AND INDEX < FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 09:15:00' AS TIMESTAMPNTZ)) - 900
        ) / 3600
      ) - 4
  ), "REQUEST_TABLE_W7200_O14400_F3600_BS1800_M900_cust_id" AS (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) - 4 AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) - 4 - 2 AS "__FB_FIRST_TILE_INDEX"
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
      "T0"."_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400" AS "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "POINT_IN_TIME",
        "cust_id",
        SUM(value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9) AS "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
        FROM "REQUEST_TABLE_W7200_O14400_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 AS TILE
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
        FROM "REQUEST_TABLE_W7200_O14400_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 AS TILE
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
    "_fb_internal_cust_id_window_w7200_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9_o14400"
  FROM _FB_AGGREGATED AS AGG
))
);
