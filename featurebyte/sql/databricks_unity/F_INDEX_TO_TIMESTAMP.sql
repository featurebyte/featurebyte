CREATE OR REPLACE FUNCTION F_INDEX_TO_TIMESTAMP(tile_index INTEGER, time_modulo_frequency_seconds INTEGER, blind_spot_seconds INTEGER, frequency_minute INTEGER)
  RETURNS STRING
  LANGUAGE SQL
    return to_varchar(
      dateadd(
        second,
        time_modulo_frequency_seconds - blind_spot_seconds,
        TO_TIMESTAMP(tile_index*frequency_minute*60)
      ),
      "yyyy-MM-dd'T'kk:mm:ss.SSS'Z'"
    );
