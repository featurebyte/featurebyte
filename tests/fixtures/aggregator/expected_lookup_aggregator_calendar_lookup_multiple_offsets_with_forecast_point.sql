SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_CUSTOMER_ID_lookup_col_float_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_input_1",
  "T1"."_fb_internal_CUSTOMER_ID_lookup_col_float_1_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_1_input_1",
  "T2"."_fb_internal_CUSTOMER_ID_lookup_col_float_-1_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_-1_input_1"
FROM REQUEST_TABLE
LEFT JOIN (
  SELECT
    "CUSTOMER_ID",
    "date",
    ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_col_float_input_1") AS "_fb_internal_CUSTOMER_ID_lookup_col_float_input_1"
  FROM (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "date",
      "col_float" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_input_1"
    FROM (
      SELECT
        "date" AS "date",
        "cust_id" AS "cust_id",
        "col_float" AS "col_float"
      FROM "db"."public"."calendar_table"
    )
  )
  GROUP BY
    "date",
    "CUSTOMER_ID"
) AS T0
  ON TO_CHAR(DATE_TRUNC('day', REQ."FORECAST_POINT"), 'YYYY-MM-DD') = T0."date"
  AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
LEFT JOIN (
  SELECT
    "CUSTOMER_ID",
    "date",
    ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_col_float_1_input_1") AS "_fb_internal_CUSTOMER_ID_lookup_col_float_1_input_1"
  FROM (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "date",
      "col_float" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_1_input_1"
    FROM (
      SELECT
        "date" AS "date",
        "cust_id" AS "cust_id",
        "col_float" AS "col_float"
      FROM "db"."public"."calendar_table"
    )
  )
  GROUP BY
    "date",
    "CUSTOMER_ID"
) AS T1
  ON TO_CHAR(DATE_ADD(DATE_TRUNC('day', REQ."FORECAST_POINT"), -86400, 'SECOND'), 'YYYY-MM-DD') = T1."date"
  AND REQ."CUSTOMER_ID" = T1."CUSTOMER_ID"
LEFT JOIN (
  SELECT
    "CUSTOMER_ID",
    "date",
    ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_col_float_-1_input_1") AS "_fb_internal_CUSTOMER_ID_lookup_col_float_-1_input_1"
  FROM (
    SELECT
      "cust_id" AS "CUSTOMER_ID",
      "date",
      "col_float" AS "_fb_internal_CUSTOMER_ID_lookup_col_float_-1_input_1"
    FROM (
      SELECT
        "date" AS "date",
        "cust_id" AS "cust_id",
        "col_float" AS "col_float"
      FROM "db"."public"."calendar_table"
    )
  )
  GROUP BY
    "date",
    "CUSTOMER_ID"
) AS T2
  ON TO_CHAR(DATE_ADD(DATE_TRUNC('day', REQ."FORECAST_POINT"), 86400, 'SECOND'), 'YYYY-MM-DD') = T2."date"
  AND REQ."CUSTOMER_ID" = T2."CUSTOMER_ID"
