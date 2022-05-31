CREATE OR REPLACE FUNCTION F_INDEX_TO_TIMESTAMP(tile_index INTEGER, window_end_seconds INTEGER, frequency_minute INTEGER)
  RETURNS VARCHAR
  AS
  $$
      select to_varchar(dateadd(second, tile_index*frequency_minute*60, dateadd(second, window_end_seconds, '1970-01-01 00:00:00'::timestamp_ntz)), 'YYYY-MM-DD HH24:MI:SS')
  $$
  ;
