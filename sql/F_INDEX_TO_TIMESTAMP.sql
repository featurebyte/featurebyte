CREATE FUNCTION F_INDEX_TO_TIMESTAMP(tile_index INTEGER, window_end_minute INTEGER, frequency_minute INTEGER)
  RETURNS VARCHAR
  AS
  $$
      select to_varchar(dateadd(minute, tile_index*frequency_minute, dateadd(minute, window_end_minute, '1970-01-01 00:00:00'::timestamp_ntz)), 'YYYY-MM-DD HH24:MI:SS')
  $$
  ;
