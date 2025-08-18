WITH REQUEST_TABLE AS (
  SELECT
    CAST('2022-04-20 10:00:00' AS TIMESTAMP) AS "POINT_IN_TIME",
    'C1' AS "CUSTOMER_ID"
), _FB_AGGREGATED AS (
  SELECT
    REQ."POINT_IN_TIME",
    REQ."CUSTOMER_ID",
    "T0"."_fb_internal_CUSTOMER_ID_lookup_a_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_a_input_1"
  FROM REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "CUSTOMER_ID",
      "snapshot_date",
      ANY_VALUE("_fb_internal_CUSTOMER_ID_lookup_a_input_1") AS "_fb_internal_CUSTOMER_ID_lookup_a_input_1"
    FROM (
      SELECT
        "cust_id" AS "CUSTOMER_ID",
        "snapshot_date",
        "a" AS "_fb_internal_CUSTOMER_ID_lookup_a_input_1"
      FROM (
        SELECT
          "snapshot_date" AS "snapshot_date",
          "cust_id" AS "cust_id",
          "a" AS "a"
        FROM "db"."public"."customer_snapshot"
      )
    )
    GROUP BY
      "snapshot_date",
      "CUSTOMER_ID"
  ) AS T0
    ON DATE_TRUNC('day', REQ."POINT_IN_TIME") = T0."snapshot_date"
    AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
)
SELECT
  AGG."POINT_IN_TIME",
  AGG."CUSTOMER_ID",
  CAST("_fb_internal_CUSTOMER_ID_lookup_a_input_1" AS DOUBLE) AS "lookup_feature_a"
FROM _FB_AGGREGATED AS AGG
