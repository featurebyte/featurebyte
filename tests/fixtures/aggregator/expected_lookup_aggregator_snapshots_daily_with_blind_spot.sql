SELECT
  POINT_IN_TIME,
  cust_id,
  "T0"."_fb_internal_CUSTOMER_ID_lookup_a_input_1" AS "_fb_internal_CUSTOMER_ID_lookup_a_input_1"
FROM REQUEST_TABLE
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
  ON DATE_ADD(
    DATE_ADD(
      DATE_TRUNC('day', REQ."__FB_CRON_JOB_SCHEDULE_DATETIME_10 * * * *_Etc/UTC_None"),
      -86400,
      'SECOND'
    ),
    -259200,
    'SECOND'
  ) = T0."snapshot_date"
  AND REQ."CUSTOMER_ID" = T0."CUSTOMER_ID"
