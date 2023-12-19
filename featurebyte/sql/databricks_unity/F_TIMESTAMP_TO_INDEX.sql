CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX(event_timestamp TIMESTAMP, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS INTEGER
  LANGUAGE SQL
    return floor(
      unix_timestamp(
        dateadd(second, -(time_modulo_frequency_seconds - blind_spot_seconds), event_timestamp)
      ) / (frequency_minute*60)
     );
