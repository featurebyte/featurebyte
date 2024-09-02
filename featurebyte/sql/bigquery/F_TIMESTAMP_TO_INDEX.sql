CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_TIMESTAMP_TO_INDEX`(event_timestamp DATETIME, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS INTEGER
  AS (
    CAST(
      FLOOR(
        UNIX_SECONDS(
          TIMESTAMP(DATETIME_SUB(event_timestamp, INTERVAL (time_modulo_frequency_seconds - blind_spot_seconds) SECOND))
        ) / (frequency_minute*60)
      ) AS INTEGER
    )
  );
