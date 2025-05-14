CREATE OR REPLACE FUNCTION VECTOR_AGGREGATE_AVG(vector ARRAY, vector_count FLOAT)
    RETURNS TABLE (vector_agg_result ARRAY)
    LANGUAGE python
    RUNTIME_VERSION=3.11
    HANDLER='VectorAggregateAvg'
as $$
class VectorAggregateAvg:
    def __init__(self):
      self._sum_array = []
      self._count = 0.0

    def _calculate_average(self):
      avg_array = []
      for i, curr_sum in enumerate(self._sum_array):
        avg_array.append(self._sum_array[i] / self._count)
      return [(avg_array,)]

    def process(self, vector, vector_count):
      self._count += vector_count
      if not self._sum_array:
        self._sum_array = vector
        return

      assert len(self._sum_array) == len(vector)
      for i in range(len(vector)):
        self._sum_array[i] += vector[i]

    def end_partition(self):
      return self._calculate_average()
$$
;
