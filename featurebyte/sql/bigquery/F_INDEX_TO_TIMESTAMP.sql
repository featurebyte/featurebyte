CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_INDEX_TO_TIMESTAMP`(tile_index INTEGER, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS STRING
  AS (
    FORMAT_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%E3SZ',
      DATETIME_ADD(
        TIMESTAMP_SECONDS(tile_index*frequency_minute*60),
        INTERVAL (time_modulo_frequency_seconds - blind_spot_seconds) SECOND
      )
    )
  );
