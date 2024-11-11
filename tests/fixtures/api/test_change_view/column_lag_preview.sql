SELECT
  CAST("col_text" AS VARCHAR) AS "col_text",
  CAST("new_effective_timestamp" AS VARCHAR) AS "new_effective_timestamp",
  CAST("past_effective_timestamp" AS VARCHAR) AS "past_effective_timestamp",
  "new_col_int" AS "new_col_int",
  "past_col_int" AS "past_col_int",
  CAST(LAG("col_text", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS VARCHAR) AS "lag_col_text",
  CAST(LAG("new_effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS VARCHAR) AS "lag_new_effective_timestamp",
  CAST(LAG("past_effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS VARCHAR) AS "lag_past_effective_timestamp",
  LAG("new_col_int", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_new_col_int",
  LAG("past_col_int", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_past_col_int"
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
LIMIT 10
