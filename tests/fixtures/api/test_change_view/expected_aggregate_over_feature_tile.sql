WITH __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    *
  FROM (
    SELECT
      "new_effective_timestamp" AS "new_effective_timestamp",
      "col_text" AS "col_text"
    FROM (
      SELECT
        "col_text",
        "new_effective_timestamp",
        "new_col_int",
        LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
        "past_col_int"
      FROM (
        SELECT
          "col_text",
          "effective_timestamp" AS "new_effective_timestamp",
          "col_int" AS "new_col_int",
          LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_col_int"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "is_active" AS "is_active",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "date_of_birth" AS "date_of_birth",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
        )
        QUALIFY
          "col_int" IS DISTINCT FROM LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
      )
    )
  )
  WHERE
    "new_effective_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMP)
    AND "new_effective_timestamp" < CAST(__FB_END_DATE AS TIMESTAMP)
)
SELECT
  index,
  "col_text",
  COUNT(*) AS value_count_6a4db28abbaea0a4ec3014fd4af6277642548acd
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "new_effective_timestamp") AS TIMESTAMP), 0, 0, 1440) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "col_text"
