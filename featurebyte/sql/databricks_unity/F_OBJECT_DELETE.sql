CREATE OR REPLACE FUNCTION OBJECT_DELETE(input_map MAP<STRING, DOUBLE>, key_to_delete STRING)
  RETURNS MAP<STRING, DOUBLE>
  LANGUAGE SQL
    return map_filter(input_map, (k, v) -> k != key_to_delete);
