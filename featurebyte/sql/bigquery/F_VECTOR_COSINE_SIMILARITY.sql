CREATE OR REPLACE FUNCTION `{project}.{dataset}.F_VECTOR_COSINE_SIMILARITY`(vector1 ARRAY<FLOAT64>, vector2 ARRAY<FLOAT64>)
  RETURNS FLOAT64
  LANGUAGE js
AS r"""

  if (!vector1 || !vector2) {{
    return 0;
  }}

  if (vector1.length != vector2.length) {{
    throw new Error("vectors are of different length");
  }}

  if (vector1.length == 0 || vector2.length == 0) {{
    return 0;
  }}

  function dot_product(arr1, arr2) {{
    return arr1.reduce((sum, val, i) => sum + val * arr2[i], 0);
  }}

  function euclidean_norm(arr) {{
    return Math.sqrt(arr.reduce((sum, val) => sum + val * val, 0));
  }}

  // Normalize arr1 and arr2 separately
  norm_arr1 = euclidean_norm(vector1);
  norm_arr2 = euclidean_norm(vector2);

  // Calculate the dot product
  dot_product_value = dot_product(vector1, vector2);

  return dot_product_value / (norm_arr1 * norm_arr2);
""";
