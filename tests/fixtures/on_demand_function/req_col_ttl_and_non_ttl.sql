CREATE FUNCTION udf_func(x_1 TIMESTAMP, x_2 DOUBLE, r_1 TIMESTAMP)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT ''
AS $$
import datetime
import json
import numpy as np
import pandas as pd
import scipy as sp


def user_defined_function(
    col_1: pd.Timestamp, col_2: float, request_col_1: pd.Timestamp
) -> float:
    # col_1: __feature_V231227__part0
    # col_2: __feature_V231227__part1
    # request_col_1: POINT_IN_TIME
    feat_1 = pd.to_datetime(col_1, utc=True)
    feat_2 = pd.to_datetime(request_col_1, utc=True)
    feat_3 = (
        np.nan
        if pd.isna(((feat_2 + (feat_2 - feat_2)) - feat_1))
        else ((feat_2 + (feat_2 - feat_2)) - feat_1).total_seconds() // 86400
    )
    feat_4 = np.nan if pd.isna(feat_3) or pd.isna(col_2) else feat_3 + col_2
    return feat_4

return user_defined_function(x_1, x_2, r_1)
$$
