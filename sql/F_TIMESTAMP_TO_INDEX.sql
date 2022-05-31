CREATE FUNCTION F_TIMESTAMP_TO_INDEX(ts_str VARCHAR, window_end_seconds INTEGER, frequency_minute INTEGER)
  RETURNS INTEGER
  AS
  $$
      select floor(timediff(second, dateadd(second, window_end_seconds, '1970-01-01 00:00:00'::timestamp_ntz), ts_str::timestamp_ntz)/(frequency_minute*60)) as index
  $$
  ;
