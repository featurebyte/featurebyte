CREATE FUNCTION udf_func(x_1 DOUBLE, x_2 DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT ''
AS $$
import datetime
import json
import numpy as np
import pandas as pd
import scipy as sp


def user_defined_function(col_1: float, col_2: float) -> float:
    # col_1: __feature_V231229__part0
    # col_2: __feature_V231229__part1
    feat_1 = np.nan if pd.isna(col_1) or pd.isna(col_2) else col_1 + col_2
    return feat_1

output = user_defined_function(x_1, x_2)
return None if pd.isnull(output) else output
$$