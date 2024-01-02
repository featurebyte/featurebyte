CREATE FUNCTION odf_func(x_1 STRING, x_2 DOUBLE, r_1 STRING)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT ''
AS $$
import json
import numpy as np
import pandas as pd
import scipy as sp


def on_demand_feature_function(
    col_1: str, col_2: float, request_col_1: str
) -> float:
    # col_1: __feature_V231227__part0
    # col_2: __feature_V231227__part1
    # request_col_1: POINT_IN_TIME
    feat_1 = pd.to_datetime(col_1, utc=True)
    feat_2 = pd.to_datetime(request_col_1, utc=True)
    feat_3 = (
        np.nan
        if pd.isna(((feat_2 + (feat_2 - feat_2)) - feat_1))
        else ((feat_2 + (feat_2 - feat_2)) - feat_1).seconds // 86400
    )
    feat_4 = np.nan if pd.isna(feat_3) or pd.isna(col_2) else feat_3 + col_2
    return feat_4

return on_demand_feature_function(x_1, x_2, r_1)
$$
