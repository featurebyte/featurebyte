WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_int" AS "input_col_sum_7243332f802124bbc68aa258dbf80f53599a0c5b"
    FROM "sf_database"."sf_schema"."sf_table"
    WHERE
      (
        "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
        AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
      )
      AND (
        "col_text" >= TO_CHAR(DATEADD(DAY, -7, CAST(__FB_START_DATE AS TIMESTAMP)), '%Y-%m-%d %H:%M:%S')
        AND "col_text" <= TO_CHAR(DATEADD(DAY, 7, CAST(__FB_END_DATE AS TIMESTAMP)), '%Y-%m-%d %H:%M:%S')
      )
  )
  WHERE
    "event_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "event_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_7243332f802124bbc68aa258dbf80f53599a0c5b") AS value_sum_7243332f802124bbc68aa258dbf80f53599a0c5b
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
