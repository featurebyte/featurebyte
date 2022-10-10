CREATE OR REPLACE FUNCTION
  F_TIMESTAMP_TO_INDEX(event_timestamp TIMESTAMP, time_modulo_frequency_seconds FLOAT, blind_spot_seconds FLOAT, frequency_minute FLOAT)
  RETURNS INT
  CONTAINS SQL
  RETURN
  select index from (
    select floor(period_in_seconds / (frequency_minute*60)) as index from (
      select unix_timestamp(adjusted_ts) as period_in_seconds from (
        select dateadd(second, -(time_modulo_frequency_seconds - blind_spot_seconds), event_timestamp) as adjusted_ts from (
          select (time_modulo_frequency_seconds - blind_spot_seconds) as offset
        )
      )
    )
  );
