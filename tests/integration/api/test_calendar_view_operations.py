"""
Integration tests for CalendarView operations
"""


def test_calendar_view(calendar_table):
    """
    Test that CalendarView can be created and queried correctly
    """
    view = calendar_table.get_view()
    view = view[view["series_id_col"] == 1]
    view = view[["calendar_datetime_col", "series_id_col", "public_holiday_name"]]
    df_preview = view.preview(limit=10000)
    df_preview = df_preview.sort_values("calendar_datetime_col").reset_index(drop=True)
    actual = df_preview.iloc[:5].to_dict(orient="list")
    expected = {
        "calendar_datetime_col": [
            "2001|01|01",
            "2001|01|02",
            "2001|01|03",
            "2001|01|04",
            "2001|01|05",
        ],
        "series_id_col": [1, 1, 1, 1, 1],
        "public_holiday_name": ["New Year's Day", None, None, None, None],
    }
    assert actual == expected


def test_time_series_view_join_calendar_view(
    time_series_table,
    calendar_table,
):
    """
    Test joining TimeSeriesView with CalendarView

    Each time series row is joined to its corresponding calendar row based on:
    - entity match: user_id_col (User entity) == series_id_col (User entity)
    - temporal match: reference_datetime_col == calendar_datetime_col

    Focuses on a single series (S0) and verifies that only dates marked as public holidays
    receive a non-null public_holiday_name, while all other dates remain null.
    """
    time_series_view = time_series_table.get_view()
    calendar_view = calendar_table.get_view()

    # Focus on a single series to make assertions deterministic
    joined_view = time_series_view[time_series_view["series_id_col"] == "S0"].join(
        calendar_view, rsuffix="_from_calendar"
    )

    # Preview all rows for this series with a large limit
    df = joined_view.preview(limit=10000)

    # Sort by reference datetime
    df = df.sort_values("reference_datetime_col").reset_index(drop=True)

    assert len(df) > 0

    # Verify first 5 rows: only Jan 1 (New Year's Day) should have a holiday name
    actual = (
        df[["reference_datetime_col", "public_holiday_name_from_calendar"]]
        .head(5)
        .to_dict(orient="records")
    )
    assert actual == [
        {
            "reference_datetime_col": "2001|01|01",
            "public_holiday_name_from_calendar": "New Year's Day",
        },
        {"reference_datetime_col": "2001|01|02", "public_holiday_name_from_calendar": None},
        {"reference_datetime_col": "2001|01|03", "public_holiday_name_from_calendar": None},
        {"reference_datetime_col": "2001|01|04", "public_holiday_name_from_calendar": None},
        {"reference_datetime_col": "2001|01|05", "public_holiday_name_from_calendar": None},
    ]
