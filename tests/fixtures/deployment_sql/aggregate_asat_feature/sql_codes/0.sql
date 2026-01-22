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
    "T0"."_fb_internal_gender_as_at_count_None_col_boolean_None_project_1" AS "_fb_internal_gender_as_at_count_None_col_boolean_None_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      REQ."POINT_IN_TIME" AS "POINT_IN_TIME",
      REQ."gender" AS "gender",
      COUNT(*) AS "_fb_internal_gender_as_at_count_None_col_boolean_None_project_1"
    FROM "REQUEST_TABLE_POINT_IN_TIME_gender" AS REQ
    INNER JOIN (
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
      ON REQ."gender" = SCD."col_boolean"
      AND (
        SCD."effective_timestamp" <= REQ."POINT_IN_TIME"
        AND (
          SCD."end_timestamp" > REQ."POINT_IN_TIME" OR SCD."end_timestamp" IS NULL
        )
      )
    GROUP BY
      REQ."POINT_IN_TIME",
      REQ."gender"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."gender" = T0."gender"
)
SELECT
  AGG."gender",
  CAST("_fb_internal_gender_as_at_count_None_col_boolean_None_project_1" AS BIGINT) AS "asat_gender_count",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG