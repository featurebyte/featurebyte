CREATE OR REPLACE AGGREGATE FUNCTION `{project}.{dataset}.VECTOR_AGGREGATE_MAX`(vector ARRAY<FLOAT64>)
    RETURNS ARRAY<FLOAT64>
    LANGUAGE js
as r'''
  export function initialState() {{
    return {{max_array: []}}
  }}
  export function aggregate(state, x) {{
    if (state.max_array.length === 0) {{
      state.max_array = x;
      return;
    }}
    for (var i = 0; i < x.length; i++) {{
      if (x[i] > state.max_array[i]) {{
        state.max_array[i] = x[i];
      }}
    }}
  }}
  export function merge(state, partialState) {{
    if (state.max_array.length === 0) {{
      state.max_array = partialState.max_array;
      return;
    }}
    for (var i = 0; i < partialState.max_array.length; i++) {{
      if (partialState.max_array[i] > state.max_array[i]) {{
        state.max_array[i] = partialState.max_array[i];
      }}
    }}
  }}
  export function finalize(state) {{
    return state.max_array;
  }}
''';
