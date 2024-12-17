SELECT
  CONVERT_TIMEZONE(
    'UTC',
    TO_TIMESTAMP_TZ(
      CONCAT(
        TO_CHAR(TO_TIMESTAMP("ts", '%Y-%m-%d %H:%M:%S'), 'YYYY-MM-DD HH24:MI:SS'),
        ' ',
        "tz_col"
      )
    )
  ) AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a"
FROM "db"."public"."event_table"
