SELECT
  "ts" AS "ts",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(hour, DATEADD(SECOND, F_TIMEZONE_OFFSET_TO_SECOND('+08:00'), "ts")) AS "hour"
FROM "db"."public"."event_table"
