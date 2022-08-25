CREATE OR REPLACE FUNCTION F_COUNT_DICT_COSINE_SIMILARITY(counts1 variant, counts2 variant)
  RETURNS float
  LANGUAGE JAVASCRIPT
AS
$$
  if (!COUNTS1 || !COUNTS2) {
    return 0;
  }
  if (Object.keys(COUNTS1).length == 0 || Object.keys(COUNTS2).length == 0) {
    return 0;
  }
  var counts;
  var counts_other;
  if (Object.keys(COUNTS1).length < Object.keys(COUNTS2).length) {
    counts = COUNTS1;
    counts_other = COUNTS2;
  }
  else {
    counts = COUNTS2;
    counts_other = COUNTS1;
  }
  var dot_product = 0.0;
  var norm = 0.0;
  var norm_other = 0.0;
  for (const k in counts) {
    if (k in counts_other) {
      dot_product += counts[k] * counts_other[k];
    }
    norm += counts[k] * counts[k];
  }
  for (const k in counts_other) {
    norm_other += counts_other[k] * counts_other[k] ;
  }
  var cosine_sim = dot_product / (Math.sqrt(norm) * Math.sqrt(norm_other));
  return cosine_sim;
$$
;
