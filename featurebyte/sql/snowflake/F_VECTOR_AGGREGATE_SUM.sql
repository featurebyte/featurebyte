CREATE OR REPLACE FUNCTION VECTOR_AGGREGATE_SUM(vector ARRAY)
    RETURNS TABLE (vector_agg_result ARRAY)
    LANGUAGE python
    RUNTIME_VERSION=3.11
    HANDLER='VectorAggregateSum'
as $$
class VectorAggregateSum:
    def __init__(self):
      self._sum_array = []

    def process(self, vector):
      if not self._sum_array:
        self._sum_array = vector
        return

      assert len(self._sum_array) == len(vector)
      for i in range(len(vector)):
        self._sum_array[i] += vector[i]

    def end_partition(self):
      return [(self._sum_array,)]
$$
;
