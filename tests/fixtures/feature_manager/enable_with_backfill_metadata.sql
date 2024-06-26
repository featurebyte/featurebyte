ALTER TABLE TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 ADD COLUMN
value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 FLOAT;


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
        "event_timestamp" >= CAST('2022-05-15T04:45:00.000000Z' AS TIMESTAMPNTZ)
        AND "event_timestamp" < CAST('2022-05-15T06:45:00.000000Z' AS TIMESTAMPNTZ)
    )
    WHERE
      "event_timestamp" >= CAST('2022-05-15T04:45:00.000000Z' AS TIMESTAMPNTZ)
      AND "event_timestamp" < CAST('2022-05-15T06:45:00.000000Z' AS TIMESTAMPNTZ)
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

CREATE TABLE "__SESSION_TEMP_TABLE_000000000000000000000000" AS
SELECT * FROM (SELECT
  "cust_id",
  CAST('_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9' AS VARCHAR) AS "AGGREGATION_RESULT_NAME",
  "_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9" AS "VALUE"
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
      ) - 4
      AND INDEX < FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 09:15:00' AS TIMESTAMPNTZ)) - 900
        ) / 3600
      )
  ), "REQUEST_TABLE_W14400_F3600_BS1800_M900_cust_id" AS (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) AS "__FB_LAST_TILE_INDEX",
      FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) - 4 AS "__FB_FIRST_TILE_INDEX"
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
      "T0"."_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9" AS "_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
    FROM REQUEST_TABLE AS REQ
    LEFT JOIN (
      SELECT
        "POINT_IN_TIME",
        "cust_id",
        SUM(value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9) AS "_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
      FROM (
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
        FROM "REQUEST_TABLE_W14400_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) = FLOOR(TILE.INDEX / 4)
          AND REQ."cust_id" = TILE."cust_id"
        WHERE
          TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
        UNION ALL
        SELECT
          REQ."POINT_IN_TIME",
          REQ."cust_id",
          TILE.INDEX,
          TILE.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
        FROM "REQUEST_TABLE_W14400_F3600_BS1800_M900_cust_id" AS REQ
        INNER JOIN TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 AS TILE
          ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 4) - 1 = FLOOR(TILE.INDEX / 4)
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
    "_fb_internal_cust_id_window_w14400_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9"
  FROM _FB_AGGREGATED AS AGG
));


INSERT INTO online_store_377553e5920dd2db8b17f21ddd52f8b1194a780c ("cust_id", "AGGREGATION_RESULT_NAME", "VALUE", "VERSION", UPDATED_AT)
SELECT "cust_id", "AGGREGATION_RESULT_NAME", "VALUE", 0, to_timestamp('2022-05-15 10:00:05')
FROM __SESSION_TEMP_TABLE_000000000000000000000000
;
