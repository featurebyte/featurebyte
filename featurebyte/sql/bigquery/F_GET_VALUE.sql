CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_GET_VALUE`(input_map JSON, key_to_get STRING)
  RETURNS STRING
  LANGUAGE js
AS r"""
  if (!input_map || !(key_to_get in input_map)) {{
    return JSON.stringify(null);
  }}
  return JSON.stringify(input_map[key_to_get]);
r""";
