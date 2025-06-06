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
      "event_timestamp" >= CAST('2022-05-15T08:45:00' AS TIMESTAMP)
      AND "event_timestamp" < CAST('2022-06-15T09:45:00' AS TIMESTAMP)
  )
  WHERE
    "event_timestamp" >= CAST('2022-05-15T08:45:00' AS TIMESTAMP)
    AND "event_timestamp" < CAST('2022-06-15T09:45:00' AS TIMESTAMP)
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

ALTER TABLE __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 ADD COLUMN value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 FLOAT;


            merge into __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 a using __TEMP_TILE_TABLE_000000000000000000000001 b
                on a.INDEX = b.INDEX AND EQUAL_NULL(a."cust_id", b."cust_id")
                when matched then
                    update set a.created_at = current_timestamp(), a.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9 = b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9
                when not matched then
                    insert (INDEX, "cust_id", value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, CREATED_AT)
                        values (b.INDEX, b."cust_id", b.value_count_704bc9a2e9fe7b08d6c064fbacd6b3fcb0185da9, current_timestamp())
        ;
