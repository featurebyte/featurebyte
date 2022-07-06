CREATE OR REPLACE FUNCTION F_INDEX_TO_TIMESTAMP(tile_index INTEGER, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS VARCHAR
  AS
  $$
      select to_varchar(adjusted_ts, 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"') from (

        select

          (time_modulo_frequency_seconds - blind_spot_seconds) as offset,

          tile_index*frequency_minute*60 as period_in_seconds,

          TO_TIMESTAMP_NTZ(period_in_seconds) as epoch_ts,

          dateadd(second, offset, epoch_ts) as adjusted_ts
      )
  $$
  ;
