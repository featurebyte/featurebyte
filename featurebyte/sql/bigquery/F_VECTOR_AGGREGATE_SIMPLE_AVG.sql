CREATE OR REPLACE AGGREGATE FUNCTION `{project}.{dataset}.VECTOR_AGGREGATE_AVG`(vector ARRAY<FLOAT64>, vector_count FLOAT64)
    RETURNS ARRAY<FLOAT64>
    LANGUAGE js
as r'''
  export function initialState() {{
    return {{sum_array: [], total_count: 0.0}}
  }}
  export function aggregate(state, x, vector_count) {{
    state.total_count += vector_count;
    if (state.sum_array.length === 0) {{
      state.sum_array = x;
      return;
    }}
    for (var i = 0; i < x.length; i++) {{
        state.sum_array[i] += x[i];
    }}
  }}
  export function merge(state, partialState) {{
    state.total_count += partialState.total_count;
    if (state.sum_array.length === 0) {{
      state.sum_array = partialState.sum_array;
      return;
    }}
    for (var i = 0; i < partialState.sum_array.length; i++) {{
      state.sum_array[i] += partialState.sum_array[i];
    }}
  }}
  export function finalize(state) {{
    var avg_array = [];
    for (var i = 0; i < state.sum_array.length; i++) {{
      avg_array.push(state.sum_array[i] / state.total_count);
    }}
    return avg_array;
  }}
''';
