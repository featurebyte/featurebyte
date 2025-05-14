CREATE OR REPLACE FUNCTION VECTOR_AGGREGATE_SIMPLE_AVERAGE(vector ARRAY)
    RETURNS TABLE (vector_agg_result ARRAY)
    LANGUAGE python
    RUNTIME_VERSION=3.11
    HANDLER='VectorAggregateSimpleAvg'
as $$
class VectorAggregateSimpleAvg:
    def __init__(self):
      self._sum_array = []
      self._count = 0.0

    def _calculate_average(self):
      avg_array = []
      for i, curr_sum in enumerate(self._sum_array):
        avg_array.append(self._sum_array[i] / self._count)
      return [(avg_array,)]

    def process(self, vector):
      self._count += 1
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
