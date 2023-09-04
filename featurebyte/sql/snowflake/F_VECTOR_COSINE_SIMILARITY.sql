CREATE OR REPLACE FUNCTION F_VECTOR_COSINE_SIMILARITY(vector1 ARRAY, vector2 ARRAY)
  RETURNS FLOAT
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.8
  HANDLER='cosine_similarity'
AS
$$
def cosine_similarity(vector1, vector2):
  import math

  if len(vector1) == 0 or len(vector2) == 0:
    return 0

  def dot_product(arr1, arr2):
    return sum(x * y for x, y in zip(arr1, arr2))

  def euclidean_norm(arr):
    return math.sqrt(sum(x ** 2 for x in arr))

  # Normalize arr1 and arr2 separately
  norm_arr1 = euclidean_norm(vector1)
  norm_arr2 = euclidean_norm(vector2)

  # Calculate the dot product using elements up to the length of the shorter array
  shorter_array_length = min(len(vector1), len(vector1))
  dot_product_value = dot_product(vector1[:shorter_array_length], vector2[:shorter_array_length])

  return dot_product_value / (norm_arr1 * norm_arr2)
$$
;
