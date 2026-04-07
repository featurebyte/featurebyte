SELECT
  L."date" AS "date",
  L."tz_offset" AS "tz_offset",
  R."date" AS "cal1_date",
  R."tz_offset" AS "cal1_tz_offset",
  DATE_TRUNC(
    'day',
    CONVERT_TIMEZONE('UTC', L."tz_offset", TO_TIMESTAMP(L."date", 'YYYY|MM|DD'))
  ) AS "__FB_SNAPSHOTS_ADJUSTED_date"
FROM left_table AS L
JOIN right_table AS R
  ON L."user_id" = R."user_id"
