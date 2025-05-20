WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_float" AS "input_col_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460",
      "col_float" AS "input_col_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
      AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
  )
  WHERE
    "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  SUM("input_col_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460") AS sum_value_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460,
  COUNT("input_col_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460") AS count_value_avg_ce0c9886ef9c14b43e37879200b1410d9d97e460,
  SUM(
    CAST("input_col_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65" AS DOUBLE) * CAST("input_col_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65" AS DOUBLE)
  ) AS sum_value_squared_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65,
  SUM(CAST("input_col_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65" AS DOUBLE)) AS sum_value_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65,
  COUNT("input_col_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65") AS count_value_std_06e1272c0c3f2d9fe71c0a171e70e6360bf00c65
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 300, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
