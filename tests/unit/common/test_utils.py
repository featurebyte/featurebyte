"""
Test helper functions in featurebyte.common.utils
"""
import pandas as pd
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import dataframe_from_arrow_stream, dataframe_to_arrow_bytes


def test_dataframe_to_arrow_bytes():
    """
    Test dataframe_to_arrow_bytes
    """
    original_df = pd.DataFrame(
        {
            "a": range(10),
            "b": range(10),
        }
    )
    data = dataframe_to_arrow_bytes(original_df)
    output_df = dataframe_from_arrow_stream(data)
    assert_frame_equal(output_df, original_df)
