CREATE TABLE "sf_db"."sf_schema"."ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000" AS
SELECT
  "cust_id" AS "cust_id",
  CAST(FLOOR((
    DATE_PART(EPOCH_SECOND, MAX(POINT_IN_TIME)) - 300
  ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP) AS __FB_ENTITY_TABLE_END_DATE,
  DATEADD(
    microsecond,
    (
      1 * 1800 * CAST(1000000 AS BIGINT) / CAST(1 AS BIGINT)
    ) * -1,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, MIN(POINT_IN_TIME)) - 300
    ) / 1800) * 1800 + 300 - 600 AS TIMESTAMP)
  ) AS __FB_ENTITY_TABLE_START_DATE
FROM "my_request_table"
GROUP BY
  "cust_id";

ALTER TABLE TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 ADD COLUMN value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295 FLOAT;


            merge into TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 a using (
            select
                index,
                "cust_id", value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295,
                current_timestamp() as created_at
            from (SELECT
  index,
  "cust_id",
  SUM("col_float") AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CONVERT_TIMEZONE('UTC', "event_timestamp"), 300, 600, 30) AS index
  FROM (
    WITH __FB_ENTITY_TABLE_NAME AS (
      SELECT
        *
      FROM "ON_DEMAND_TILE_ENTITY_TABLE_000000000000000000000000"
    )
    SELECT
      R.*
    FROM __FB_ENTITY_TABLE_NAME
    INNER JOIN (
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
    ) AS R
      ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
      AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
      AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
  )
)
GROUP BY
  index,
  "cust_id")
        ) b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."cust_id", b."cust_id")
                when matched then
                    update set a.created_at = current_timestamp(), a.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295 = b.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
                when not matched then
                    insert (INDEX, "cust_id", value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295, CREATED_AT)
                        values (b.INDEX, b."cust_id", b.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295, current_timestamp())
        ;
