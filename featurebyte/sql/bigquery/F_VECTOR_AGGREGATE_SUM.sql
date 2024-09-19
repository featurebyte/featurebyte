CREATE OR REPLACE AGGREGATE FUNCTION `{project}.{dataset}.VECTOR_AGGREGATE_SUM`(vector ARRAY<FLOAT64>)
    RETURNS ARRAY<FLOAT64>
    LANGUAGE js
as r'''
  export function initialState() {{
    return {{sum_array: []}}
  }}
  export function aggregate(state, x) {{
    if (state.sum_array.length === 0) {{
      state.sum_array = x;
      return;
    }}
    for (var i = 0; i < x.length; i++) {{
        state.sum_array[i] += x[i];
    }}
  }}
  export function merge(state, partialState) {{
    if (state.sum_array.length === 0) {{
      state.sum_array = partialState.sum_array;
      return;
    }}
    for (var i = 0; i < partialState.sum_array.length; i++) {{
      state.sum_array[i] += partialState.sum_array[i];
    }}
  }}
  export function finalize(state) {{
    return state.sum_array;
  }}
''';
