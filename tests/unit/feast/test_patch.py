"""Unit tests for the feast.patch module."""

import pandas as pd

from featurebyte.feast.patch import DataFrameWrapper


def test_getitem():
    """Test the __getitem__ method."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    df_wrapper = DataFrameWrapper(df)
    df_wrapper.add_column_alias("a", "a_alias")
    df_wrapper.add_column_alias("b", "b_alias")

    assert df_wrapper["a_alias"].equals(pd.Series([1, 2, 3]))
    assert df_wrapper["b_alias"].equals(pd.Series([4, 5, 6]))

    output = df_wrapper[["a", "a_alias"]]
    expected = pd.DataFrame({"a": df["a"], "a_alias": df["a"]})
    assert output.equals(expected)
