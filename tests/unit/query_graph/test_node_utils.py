"""
Unit tests for the node/utils module.
"""
import pandas as pd
import pytest

from featurebyte.query_graph.node.utils import (
    filter_series_or_frame_expr,
    subset_frame_column_expr,
    subset_frame_columns_expr,
)


@pytest.mark.parametrize(
    "frame_name, column_name, expected_expression",
    [
        ("df", "col", "df['col']"),
        ("df", "col name", "df['col name']"),
        ("df", "col's name", 'df["col\'s name"]'),
        ("df", 'col"s name', "df['col\"s name']"),
    ],
)
def test_subset_frame_column_expression(frame_name, column_name, expected_expression):
    """Test subset_frame_column_expression function"""
    df = pd.DataFrame()
    df[column_name] = [1, 2, 3]
    expr = subset_frame_column_expr(frame_name, column_name)
    assert expr == expected_expression

    # evaluate the expression to get the column
    col = eval(expr)
    assert col.tolist() == [1, 2, 3]


@pytest.mark.parametrize(
    "frame_name, column_names, expected_expression",
    [
        ("df", ["col1", "col2"], "df[['col1', 'col2']]"),
        ("df", ["col1 name", "col2 name"], "df[['col1 name', 'col2 name']]"),
        ("df", ["col1's name", "col2's name"], 'df[["col1\'s name", "col2\'s name"]]'),
        ("df", ['col1"s name', 'col2"s name'], "df[['col1\"s name', 'col2\"s name']]"),
    ],
)
def test_subset_frame_columns_expression(frame_name, column_names, expected_expression):
    """Test subset_frame_columns_expression function"""
    df = pd.DataFrame()
    for col_name in column_names:
        df[col_name] = [1, 2, 3]
    expr = subset_frame_columns_expr(frame_name, column_names)
    assert expr == expected_expression

    # evaluate the expression to get the columns
    cols = eval(expr)
    pd.testing.assert_frame_equal(cols, df[column_names])


@pytest.mark.parametrize(
    "frame_name, filter_expression, expected_expression",
    [
        ("df", "mask", "df[mask]"),
        ("df", 'df["col"] > 0', 'df[df["col"] > 0]'),
        ("df", '(df["col"] > 0) & (df["col"] < 10)', 'df[(df["col"] > 0) & (df["col"] < 10)]'),
        ("df", "df['\"col\"\\'s name'] > 0", "df[df['\"col\"\\'s name'] > 0]"),
    ],
)
def test_filter_series_or_frame_expression(frame_name, filter_expression, expected_expression):
    """Test filter_series_or_frame_expression function"""
    df = pd.DataFrame({"col": [1, 2, 3], '"col"\'s name': [1, 2, 3]})
    mask = pd.Series([True, False, True])
    expr = filter_series_or_frame_expr(frame_name, filter_expression)
    assert expr == expected_expression
    _ = df, mask

    # check that the expression can be evaluated
    _ = eval(expr)
