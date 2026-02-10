CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_NORMALIZE`(counts JSON)
  RETURNS JSON
  LANGUAGE js
AS r"""
  if (!counts) {{
    return null;
  }}
  var total = Object.values(counts).reduce((partialSum, a) => partialSum + a, 0);
  if (total === 0) {{
    return {{}};
  }}
  var result = {{}};
  for (var key in counts) {{
    result[key] = counts[key] / total;
  }}
  return result;
""";
