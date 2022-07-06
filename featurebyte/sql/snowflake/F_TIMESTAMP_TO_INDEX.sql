CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX(event_timestamp_str VARCHAR, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS INTEGER
  AS
  $$
      select index from (

        select

          (time_modulo_frequency_seconds - blind_spot_seconds) as offset,

          dateadd(second, -offset, event_timestamp_str::timestamp_ntz) as adjusted_ts,

          date_part(epoch_second, adjusted_ts) as period_in_seconds,

          floor(period_in_seconds / (frequency_minute*60)) as index
      )
  $$
  ;
