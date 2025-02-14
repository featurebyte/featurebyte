SELECT
  "ts" AS "ts",
  "tz_offset" AS "tz_offset",
  "cust_id" AS "cust_id",
  "a" AS "a",
  DATE_PART(hour, DATEADD(SECOND, F_TIMEZONE_OFFSET_TO_SECOND("tz_offset"), "ts")) AS "hour"
FROM "db"."public"."event_table"
