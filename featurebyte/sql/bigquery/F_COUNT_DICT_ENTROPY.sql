CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_COUNT_DICT_ENTROPY`(counts JSON)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""
  if (!counts) {{
    return null;
  }}
  var counts_arr = Object.values(counts);
  var total = counts_arr.reduce((partialSum, a) => partialSum + Math.abs(a), 0);
  var entropy = 0.0;
  var count_length = counts_arr.length;
  for (var i = 0; i < count_length; i++) {{
    if (counts_arr[i] == 0) {{
      continue;
    }}
    var p = Math.abs(counts_arr[i]) / total;
    entropy += p * Math.log(p);
  }}
  entropy *= -1.0;
  return entropy;
""";
