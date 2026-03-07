CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_DIVIDE`(counts JSON, divisor FLOAT64)
  RETURNS JSON
  LANGUAGE js
AS r"""
  if (!counts) {{
    return null;
  }}
  if (divisor === null || divisor === 0) {{
    return null;
  }}
  var result = {{}};
  for (var key in counts) {{
    result[key] = counts[key] / divisor;
  }}
  return result;
""";
