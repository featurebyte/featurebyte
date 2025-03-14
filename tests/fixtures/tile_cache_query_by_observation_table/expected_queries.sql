CREATE TABLE "sf_db"."sf_schema"."ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000" AS
SELECT
  "cust_id" AS "cust_id",
  CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 300
  ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
  DATEADD(
    MICROSECOND,
    (
      (
        300 - 600
      ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
    ),
    CAST('1970-01-01' AS TIMESTAMP)
  ) AS __FB_ENTITY_TABLE_START_DATE
FROM "my_request_table"
GROUP BY
  "cust_id";

CREATE TABLE "__TEMP_TILE_TABLE_000000000000000000000000" AS
SELECT * FROM (
            select
                index,
                "cust_id", value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295,
                current_timestamp() as created_at
            from (WITH __FB_ENTITY_TABLE_NAME AS (
  SELECT
    *
  FROM "ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000"
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_float" AS "input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM "sf_database"."sf_schema"."sf_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295") AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id")
        );

CREATE TABLE "sf_db"."sf_schema"."ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000001" AS
SELECT
  "cust_id" AS "cust_id",
  CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 300
  ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
  DATEADD(
    MICROSECOND,
    (
      (
        300 - 600
      ) * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
    ),
    CAST('1970-01-01' AS TIMESTAMP)
  ) AS __FB_ENTITY_TABLE_START_DATE
FROM "my_request_table"
GROUP BY
  "cust_id";
