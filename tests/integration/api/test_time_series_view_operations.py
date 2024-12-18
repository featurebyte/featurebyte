"""
Integration tests for TimeSeriesView operations
"""


def test_times_series_view(time_series_table):
    """
    Test TimeSeriesView
    """
    view = time_series_table.get_view()
    view = view[view["series_id_col"] == "S0"]
    df_preview = view.preview()
    actual = df_preview.to_dict(orient="list")
    expected = {
        "reference_datetime_col": [
            "20010101",
            "20010102",
            "20010103",
            "20010104",
            "20010105",
            "20010106",
            "20010107",
            "20010108",
            "20010109",
            "20010110",
        ],
        "series_id_col": ["S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0", "S0"],
        "value_col": [0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09],
    }
    assert actual == expected
