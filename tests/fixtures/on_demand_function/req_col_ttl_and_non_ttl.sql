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
    feat_1 = pd.NaT if col_1 is None else pd.to_datetime(col_1, utc=True)
    feat_2 = (
        pd.NaT
        if request_col_1 is None
        else pd.to_datetime(request_col_1, utc=True)
    )
    feat_3 = pd.to_datetime(feat_2, utc=True) - pd.to_datetime(feat_2, utc=True)
    feat_4 = pd.to_datetime(feat_2, utc=True) + pd.to_timedelta(feat_3)
    feat_5 = pd.to_datetime(feat_4, utc=True) - pd.to_datetime(feat_1, utc=True)
    feat_6 = (
        np.nan
        if pd.isna(feat_5)
        else pd.to_timedelta(feat_5).total_seconds() // 86400
    )
    feat_7 = np.nan if pd.isna(feat_6) or pd.isna(col_2) else feat_6 + col_2
    return feat_7

output = user_defined_function(x_1, x_2, r_1)
return None if pd.isnull(output) else output
$$
