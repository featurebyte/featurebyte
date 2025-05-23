CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000001" AS
SELECT * FROM (
            select
                index,
                "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9,
                current_timestamp() as created_at
            from (WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      "event_timestamp" >= CAST('2022-05-15T05:45:00' AS TIMESTAMP)
      AND "event_timestamp" < CAST('2022-05-15T06:45:00' AS TIMESTAMP)
  )
  WHERE
    "event_timestamp" >= CAST('2022-05-15T05:45:00' AS TIMESTAMP)
    AND "event_timestamp" < CAST('2022-05-15T06:45:00' AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 900, 1800, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id")
        );

ALTER TABLE TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 ADD COLUMN value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 FLOAT;


            merge into TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 a using __TEMP_TILE_TABLE_000000000000000000000001 b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."cust_id", b."cust_id")
                when matched then
                    update set a.created_at = current_timestamp(), a.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 = b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
                when not matched then
                    insert (INDEX, "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, CREATED_AT)
                        values (b.INDEX, b."cust_id", b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, current_timestamp())
        ;

CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000002" AS
SELECT * FROM (
            select
                index,
                "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9,
                current_timestamp() as created_at
            from (WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      "event_timestamp" >= CAST('2022-05-15T08:45:00' AS TIMESTAMP)
      AND "event_timestamp" < CAST('2022-05-15T09:45:00' AS TIMESTAMP)
  )
  WHERE
    "event_timestamp" >= CAST('2022-05-15T08:45:00' AS TIMESTAMP)
    AND "event_timestamp" < CAST('2022-05-15T09:45:00' AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  COUNT(*) AS value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 900, 1800, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id")
        );

ALTER TABLE TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 ADD COLUMN value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 FLOAT;


            merge into TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9 a using __TEMP_TILE_TABLE_000000000000000000000002 b
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
      CAST('2022-05-15 10:15:00' AS TIMESTAMP) AS POINT_IN_TIME,
      "cust_id" AS "cust_id"
    FROM TILE_COUNT_704BC9A2E9FE7B08D6C064FBACD6B3FCB0185DA9
    WHERE
      INDEX >= CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 10:15:00' AS TIMESTAMP)) - 900
        ) / 3600
      ) AS BIGINT) - 4
      AND INDEX < CAST(FLOOR(
        (
          DATE_PART(EPOCH_SECOND, CAST('2022-05-15 10:15:00' AS TIMESTAMP)) - 900
        ) / 3600
      ) AS BIGINT)
  ), "REQUEST_TABLE_W14400_F3600_BS1800_M900_cust_id" AS (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) AS BIGINT) AS __FB_LAST_TILE_INDEX,
      CAST(FLOOR((
        DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 900
      ) / 3600) AS BIGINT) - 4 AS __FB_FIRST_TILE_INDEX
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


INSERT INTO ONLINE_STORE_377553E5920DD2DB8B17F21DDD52F8B1194A780C ("cust_id", "AGGREGATION_RESULT_NAME", "VALUE", "VERSION", UPDATED_AT)
SELECT "cust_id", "AGGREGATION_RESULT_NAME", "VALUE", 0, CAST('2022-05-15 10:00:05' AS TIMESTAMP)
FROM __SESSION_TEMP_TABLE_000000000000000000000000
;
