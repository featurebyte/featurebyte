CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_GET_RELATIVE_FREQUENCY`(COUNTS JSON, KEY_TO_USE STRING)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""
  if (!COUNTS) {{
    return;
  }}
  if (KEY_TO_USE === undefined || KEY_TO_USE === null) {{
    return;
  }}
  if (!(KEY_TO_USE in COUNTS)) {{
    return 0;
  }}
  var counts_arr = Object.values(COUNTS);
  var total = counts_arr.reduce((partialSum, a) => partialSum + a, 0);
  var key_value = COUNTS[KEY_TO_USE]
  return key_value / total;
f""";
