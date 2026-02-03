WITH _FB_AGGREGATED AS (
  SELECT
    SCD."col_boolean" AS "gender",
    COUNT(*) AS "_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1",
    {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
  FROM (
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_text" AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      "effective_timestamp" AS "effective_timestamp",
      "end_timestamp" AS "end_timestamp",
      "date_of_birth" AS "date_of_birth",
      "created_at" AS "created_at",
      "cust_id" AS "cust_id"
    FROM "sf_database"."sf_schema"."scd_table"
  ) AS SCD
  WHERE
    SCD."effective_timestamp" <= DATEADD(MICROSECOND, -604800000000.0, {{ CURRENT_TIMESTAMP }})
    AND (
      SCD."end_timestamp" > DATEADD(MICROSECOND, -604800000000.0, {{ CURRENT_TIMESTAMP }})
      OR SCD."end_timestamp" IS NULL
    )
  GROUP BY
    SCD."col_boolean"
)
SELECT
  AGG."gender",
  CAST("_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1" AS BIGINT) AS "asat_gender_count_7d_ago",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG