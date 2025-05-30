CREATE OR REPLACE FUNCTION F_VECTOR_COSINE_SIMILARITY(vector1 ARRAY, vector2 ARRAY)
  RETURNS FLOAT
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.11
  HANDLER='cosine_similarity'
AS
$$
def cosine_similarity(vector1, vector2):
  import math

  if vector1 is None or vector2 is None:
    return 0

  if len(vector1) != len(vector2):
    raise ValueError("vectors are of different length")

  if len(vector1) == 0 or len(vector2) == 0:
    return 0

  def dot_product(arr1, arr2):
    return sum(x * y for x, y in zip(arr1, arr2))

  def euclidean_norm(arr):
    return math.sqrt(sum(x ** 2 for x in arr))

  # Normalize arr1 and arr2 separately
  norm_arr1 = euclidean_norm(vector1)
  norm_arr2 = euclidean_norm(vector2)

  # Calculate the dot product
  dot_product_value = dot_product(vector1, vector2)

  return dot_product_value / (norm_arr1 * norm_arr2)
$$
;
