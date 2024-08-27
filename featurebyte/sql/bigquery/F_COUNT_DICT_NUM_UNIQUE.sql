CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_NUM_UNIQUE`(counts JSON)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""
  if (!counts) {{
    return 0;
  }}
  return Object.keys(counts).length;
""";
