CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_COSINE_SIMILARITY`(COUNTS1 JSON, COUNTS2 JSON)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""

  function replaceNaN(obj) {{
    if (!obj) {{
      return 0;
    }}
    // Need to use isNaN instead of Number.isNaN because the value is passed as a string.
    if (isNaN(obj)) {{
      return 0;
    }}
    return obj;
  }}

  if (!COUNTS1 || !COUNTS2) {{
    return null;
  }}
  if (Object.keys(COUNTS1).length == 0 || Object.keys(COUNTS2).length == 0) {{
    return 0;
  }}
  var counts;
  var counts_other;
  if (Object.keys(COUNTS1).length < Object.keys(COUNTS2).length) {{
    counts = COUNTS1;
    counts_other = COUNTS2;
  }}
  else {{
    counts = COUNTS2;
    counts_other = COUNTS1;
  }}
  var dot_product = 0.0;
  var norm = 0.0;
  var norm_other = 0.0;
  for (const k in counts) {{
    var v = replaceNaN(counts[k])
    if (k in counts_other) {{
      var v_other = replaceNaN(counts_other[k]);
      dot_product += v * v_other;
    }}
    norm += v * v;
  }}
  for (const k in counts_other) {{
    var v = replaceNaN(counts_other[k]);
    norm_other += v * v;
  }}
  var norm_product = Math.sqrt(norm) * Math.sqrt(norm_other);
  if (norm_product == 0) {{
    return 0;
  }}
  return dot_product / norm_product;
""";
