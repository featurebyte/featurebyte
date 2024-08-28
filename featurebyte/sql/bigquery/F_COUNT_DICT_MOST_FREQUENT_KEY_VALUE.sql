CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_MOST_FREQUENT_KEY_VALUE`(COUNTS JSON, REVERSED BOOL)
  RETURNS STRUCT<key STRING, count FLOAT64>
  LANGUAGE js
AS r"""
  if (!COUNTS) {{
    return [null, null];
  }}
  var most_frequent_key = null;
  var most_frequent_count = -Infinity;
  for (const k in COUNTS) {{
    if (!COUNTS[k]) {{
      continue;
    }}
    v = COUNTS[k];
    if (REVERSED) {{  // Set reversed as true to obtain the least frequent key
      v = -1 * v;
    }}
    if (v > most_frequent_count) {{
      most_frequent_count = v;
      most_frequent_key = k;
    }} else if (v == most_frequent_count && k < most_frequent_key) {{
      most_frequent_key = k;
    }}
  }}
  if (most_frequent_key === null) {{
    return {{key: null, count: null}};
  }}
  return {{key: most_frequent_key, count: most_frequent_count}};
""";
