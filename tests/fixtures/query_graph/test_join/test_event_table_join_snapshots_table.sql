SELECT
  L."event_id" AS "event_id",
  L."ts" AS "ts",
  R."cust_id" AS "cust_id",
  R."a" AS "a"
FROM (
  SELECT
    "ts" AS "ts",
    "cust_id" AS "cust_id",
    "order_id" AS "order_id",
    "order_method" AS "order_method",
    TO_CHAR(
      DATE_ADD(
        DATE_ADD(DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'Asia/Singapore', "ts")), -86400, 'SECOND'),
        -259200,
        'SECOND'
      ),
      'YYYYMMDD'
    ) AS "__FB_SNAPSHOTS_ADJUSTED_ts"
  FROM "db"."public"."event_table"
) AS L
INNER JOIN (
  SELECT
    "snapshot_date",
    "cust_id",
    ANY_VALUE("a") AS "a"
  FROM (
    SELECT
      "snapshot_date" AS "snapshot_date",
      "cust_id" AS "cust_id",
      "a" AS "a"
    FROM "db"."public"."customer_snapshot"
  )
  GROUP BY
    "cust_id",
    "snapshot_date"
) AS R
  ON L."cust_id" = R."cust_id" AND L."__FB_SNAPSHOTS_ADJUSTED_ts" = R."snapshot_date"
