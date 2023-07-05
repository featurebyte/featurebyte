"""
Dataframe helpers
"""
import json

import pandas as pd


def apply_agg_func_on_filtered_dataframe(agg_func, category, df_filtered, variable_column_name):
    """
    Helper function to apply an aggregation function on a dataframe filtered to contain only the
    data within the feature window for a specific entity
    """
    # Handle no category
    if category is None:
        return agg_func(df_filtered[variable_column_name])

    # Handle category
    out = {}
    category_vals = df_filtered[category].unique()
    for category_val in category_vals:
        if pd.isnull(category_val):
            category_val = "__MISSING__"
            category_mask = df_filtered[category].isnull()
        else:
            category_mask = df_filtered[category] == category_val
        feature_value = agg_func(df_filtered[category_mask][variable_column_name])
        out[category_val] = feature_value
    if not out:
        return None
    return json.dumps(pd.Series(out).to_dict())
