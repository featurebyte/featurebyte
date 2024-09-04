CREATE OR REPLACE FUNCTION `{project}.{dataset}.OBJECT_DELETE`(input_map JSON, key_to_delete STRING)
  RETURNS JSON
  AS (
    JSON_REMOVE(input_map, '$.' || key_to_delete)
  )
