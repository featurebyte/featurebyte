WITH DEPLOYMENT_REQUEST_TABLE AS (
  SELECT
    REQ."cust_id",
    {{ CURRENT_TIMESTAMP }} AS POINT_IN_TIME
  FROM (
    SELECT DISTINCT
      "col_text" AS "cust_id"
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
      NOT "col_text" IS NULL
  ) AS REQ
), _FB_AGGREGATED AS (
  SELECT
    REQ."cust_id",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_lookup_col_boolean_project_1" AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
  FROM DEPLOYMENT_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      SCD."col_text" AS "cust_id",
      ANY_VALUE(SCD."col_boolean") AS "_fb_internal_cust_id_lookup_col_boolean_project_1"
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
  ) AS T0
    ON REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."cust_id",
  "_fb_internal_cust_id_lookup_col_boolean_project_1" AS "some_lookup_feature",
  {{ CURRENT_TIMESTAMP }} AS "POINT_IN_TIME"
FROM _FB_AGGREGATED AS AGG