CREATE OR REPLACE FUNCTION VECTOR_AGGREGATE_MAX(vector ARRAY)
    RETURNS TABLE (vector_agg_result ARRAY)
    LANGUAGE python
    RUNTIME_VERSION=3.11
    HANDLER='VectorAggregateMax'
as $$
class VectorAggregateMax:
    def __init__(self):
      self._max_array = []

    def process(self, vector):
      if not self._max_array:
        self._max_array = vector
        return

      assert len(self._max_array) == len(vector)
      for i in range(len(vector)):
        if vector[i] > self._max_array[i]:
          self._max_array[i] = vector[i]

    def end_partition(self):
      return [(self._max_array,)]
$$
;
