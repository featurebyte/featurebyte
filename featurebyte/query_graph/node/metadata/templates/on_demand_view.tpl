import datetime
import json
import numpy as np
import pandas as pd
import scipy as sp


def {{function_name}}({{input_df_name}}: pd.DataFrame) -> pd.DataFrame:
    {{output_df_name}} = pd.DataFrame()
    {{statements | indent(4)}}
    return {{output_df_name}}
