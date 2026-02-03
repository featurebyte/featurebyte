WITH _FB_AGGREGATED AS (
  SELECT
    SCD."col_text" AS "cust_id",
    ANY_VALUE(SCD."col_boolean") AS "_fb_internal_cust_id_lookup_col_boolean_project_1",
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
    SCD."effective_timestamp" <= {{ CURRENT_TIMESTAMP }}
    AND (
      SCD."end_timestamp" > {{ CURRENT_TIMESTAMP }} OR SCD."end_timestamp" IS NULL
    )
  GROUP BY
    SCD."col_text"
)
SELECT
  AGG."cust_id",
  "_fb_internal_cust_id_lookup_col_boolean_project_1" AS "some_lookup_feature",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG