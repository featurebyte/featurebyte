SELECT
  CAST("col_text" AS VARCHAR) AS "col_text",
  CAST("new_effective_timestamp" AS VARCHAR) AS "new_effective_timestamp",
  CAST("past_effective_timestamp" AS VARCHAR) AS "past_effective_timestamp",
  CAST("new_created_at" AS VARCHAR) AS "new_created_at",
  CAST("past_created_at" AS VARCHAR) AS "past_created_at"
FROM (
  SELECT
    "col_text",
    "new_effective_timestamp",
    "new_created_at",
    LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
    "past_created_at"
  FROM (
    SELECT
      "col_text",
      "effective_timestamp" AS "new_effective_timestamp",
      "created_at" AS "new_created_at",
      LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_created_at"
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
      "created_at" IS DISTINCT FROM LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
  )
)
LIMIT 10
