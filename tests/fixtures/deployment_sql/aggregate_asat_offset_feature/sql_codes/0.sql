WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."gender",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      "col_boolean" AS "gender"
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
      WHERE
        "effective_timestamp" >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        AND "effective_timestamp" < {{ CURRENT_TIMESTAMP }}
    )
    WHERE
      NOT "col_boolean" IS NULL
  ) AS REQ
), "REQUEST_TABLE_POINT_IN_TIME_gender" AS (
  SELECT DISTINCT
    "POINT_IN_TIME",
    "gender"
  FROM DEPLOYMENT_REQUEST_TABLE
), _FB_AGGREGATED AS (
  SELECT
    REQ."gender",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1" AS "_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      SCD."col_boolean" AS "gender",
      COUNT(*) AS "_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1"
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
      SCD."effective_timestamp" <= DATEADD(MICROSECOND, -604800000000.0, REQ."POINT_IN_TIME")
      AND (
        SCD."end_timestamp" > DATEADD(MICROSECOND, -604800000000.0, REQ."POINT_IN_TIME")
        OR SCD."end_timestamp" IS NULL
      )
    GROUP BY
      SCD."col_boolean"
  ) AS T0
    ON REQ."gender" = T0."gender"
)
SELECT
  AGG."gender",
  CAST("_fb_internal_gender_as_at_count_None_col_boolean_None_7d_project_1" AS BIGINT) AS "asat_gender_count_7d_ago",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG