"""
Integration tests for CalendarView operations
"""

import numpy as np
import pandas as pd
from bson import ObjectId

from featurebyte import Context, FeatureList
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from tests.util.helper import (
    create_observation_table_by_upload,
    fb_assert_frame_equal,
)


def test_calendar_view(calendar_table):
    """
    Test that CalendarView can be created and queried correctly
    """
    view = calendar_table.get_view()
    view = view[view["series_id_col"] == 1]
    view = view[["calendar_datetime_col", "series_id_col", "public_holiday_name"]]
    df_preview = view.preview(limit=10000)
    df_preview = df_preview.sort_values("calendar_datetime_col").reset_index(drop=True)
    actual = df_preview.iloc[:5].reset_index(drop=True)
    expected = pd.DataFrame({
        "calendar_datetime_col": [
            "2001|01|01",
            "2001|01|02",
            "2001|01|03",
            "2001|01|04",
            "2001|01|05",
        ],
        "series_id_col": [1, 1, 1, 1, 1],
        "public_holiday_name": ["New Year's Day", None, None, None, None],
    })
    fb_assert_frame_equal(actual, expected)


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
    All users observe New Year's Day, so the Jan 1 assertion is unconditional.
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

    actual = df[["reference_datetime_col", "public_holiday_name_from_calendar"]].head(5)
    expected = pd.DataFrame({
        "reference_datetime_col": [
            "2001|01|01",
            "2001|01|02",
            "2001|01|03",
            "2001|01|04",
            "2001|01|05",
        ],
        "public_holiday_name_from_calendar": ["New Year's Day", None, None, None, None],
    })
    fb_assert_frame_equal(actual.reset_index(drop=True), expected)


def test_calendar_lookup_feature(calendar_table, timestamp_format_string, user_entity):
    """
    Test creating and computing a lookup feature from CalendarView.

    The feature looks up public_holiday_name for the date corresponding to the point-in-time.
    - All users observe New Year's Day (Jan 1); odd users also observe Christmas Day (Dec 25)
    - Non-holiday dates return null

    Also verifies that when a FORECAST_POINT is provided in the observation table, the lookup
    uses it as the date reference instead of POINT_IN_TIME.
    """
    view = calendar_table.get_view()
    feature_name = f"CalendarHolidayName_{ObjectId()}"
    feature = view["public_holiday_name"].as_feature(feature_name)
    feature_list = FeatureList([feature], str(ObjectId()))

    # User entity serving name is "user id"
    preview_params = pd.DataFrame([
        # New Year's Day for odd user
        {"POINT_IN_TIME": pd.Timestamp("2001-01-01 10:00:00"), "üser id": 1},
        # Non-holiday
        {"POINT_IN_TIME": pd.Timestamp("2001-06-15 12:00:00"), "üser id": 1},
        # Christmas Day
        {"POINT_IN_TIME": pd.Timestamp("2001-12-25 08:00:00"), "üser id": 1},
        # Non-holiday
        {"POINT_IN_TIME": pd.Timestamp("2001-12-26 08:00:00"), "üser id": 1},
    ])
    expected = preview_params.copy()
    expected[feature_name] = ["New Year's Day", np.nan, "Christmas Day", np.nan]
    obs_table = create_observation_table_by_upload(preview_params)
    df_features = feature_list.compute_historical_feature_table(
        obs_table, str(ObjectId())
    ).to_pandas()
    fb_assert_frame_equal(df_features, expected, sort_by_columns=["POINT_IN_TIME"])

    # Verify that FORECAST_POINT overrides POINT_IN_TIME as the date reference.
    # Observation on Dec 23, forecasting for Dec 25 -- result should be "Christmas Day".
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="Asia/Singapore",
        format_string=timestamp_format_string,
    )
    forecast_context = Context.create(
        name=f"calendar_forecast_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )
    df_fp = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-12-23 10:00:00"),
            "FORECAST_POINT": "2001|12|25",
            "üser id": 1,
        }
    ])
    obs_table = create_observation_table_by_upload(df_fp, context_name=forecast_context.name)
    df_fp_features = feature_list.compute_historical_feature_table(
        obs_table, str(ObjectId())
    ).to_pandas()
    assert df_fp_features[feature_name].iloc[0] == "Christmas Day"


def test_calendar_lookup_feature_with_offset(calendar_table, timestamp_format_string, user_entity):
    """
    Test creating calendar lookup features with positive and negative day offsets.

    Positive offset (offset=2): looks up the calendar row for (point-in-time date - 2 days).
    - POINT_IN_TIME on Jan 3 looks up Jan 1 -> "New Year's Day" (user 1 is odd)
    - POINT_IN_TIME on Jan 2 looks up Dec 31, 2000 -> null (not in dataset)
    - POINT_IN_TIME on Dec 27 looks up Dec 25 -> "Christmas Day"

    Negative offset (offset=-2): looks up the calendar row for (point-in-time date + 2 days).
    - POINT_IN_TIME on Dec 23 looks up Dec 25 -> "Christmas Day"
    - POINT_IN_TIME on Dec 30 looks up Jan 1, 2002 -> null (not in dataset)
    - POINT_IN_TIME on Dec 30, 2000 looks up Jan 1 -> "New Year's Day"

    Also verifies that when a FORECAST_POINT is provided, offsets are applied relative to it
    instead of POINT_IN_TIME, for both positive and negative offsets.
    """
    view = calendar_table.get_view()

    # Positive offset: look back 2 days
    feature_name_pos = f"CalendarHolidayNameMinus2Days_{ObjectId()}"
    feature_pos = view["public_holiday_name"].as_feature(feature_name_pos, offset=2)
    feature_list_pos = FeatureList([feature_pos], str(ObjectId()))

    preview_params_pos = pd.DataFrame([
        # 2 days after New Year's Day -> looks up Jan 1 -> "New Year's Day" (user 1 is odd)
        {"POINT_IN_TIME": pd.Timestamp("2001-01-03 10:00:00"), "üser id": 1},
        # 1 day after New Year's Day -> looks up Dec 31, 2000 -> None (not in dataset)
        {"POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"), "üser id": 1},
        # 2 days after Christmas -> looks up Dec 25 -> "Christmas Day"
        {"POINT_IN_TIME": pd.Timestamp("2001-12-27 10:00:00"), "üser id": 1},
    ])
    expected_pos = preview_params_pos.copy()
    expected_pos[feature_name_pos] = ["New Year's Day", np.nan, "Christmas Day"]
    obs_table_pos = create_observation_table_by_upload(preview_params_pos)
    df_features_pos = feature_list_pos.compute_historical_feature_table(
        obs_table_pos, str(ObjectId())
    ).to_pandas()
    fb_assert_frame_equal(df_features_pos, expected_pos, sort_by_columns=["POINT_IN_TIME"])

    # Negative offset: look forward 2 days
    feature_name_neg = f"CalendarHolidayNamePlus2Days_{ObjectId()}"
    feature_neg = view["public_holiday_name"].as_feature(feature_name_neg, offset=-2)
    feature_list_neg = FeatureList([feature_neg], str(ObjectId()))

    preview_params_neg = pd.DataFrame([
        # 2 days before Christmas -> looks up Dec 25 -> "Christmas Day"
        {"POINT_IN_TIME": pd.Timestamp("2001-12-23 10:00:00"), "üser id": 1},
        # Dec 30 -> looks up Jan 1, 2002 -> None (not in dataset)
        {"POINT_IN_TIME": pd.Timestamp("2001-12-30 10:00:00"), "üser id": 1},
        # Dec 30, 2000 -> looks up Jan 1, 2001 -> "New Year's Day"
        {"POINT_IN_TIME": pd.Timestamp("2000-12-30 10:00:00"), "üser id": 1},
    ])
    expected_neg = preview_params_neg.copy()
    expected_neg[feature_name_neg] = ["Christmas Day", np.nan, "New Year's Day"]
    obs_table_neg = create_observation_table_by_upload(preview_params_neg)
    df_features_neg = feature_list_neg.compute_historical_feature_table(
        obs_table_neg, str(ObjectId())
    ).to_pandas()
    fb_assert_frame_equal(df_features_neg, expected_neg, sort_by_columns=["POINT_IN_TIME"])

    # Verify offsets work correctly with FORECAST_POINT override
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="Asia/Singapore",
        format_string=timestamp_format_string,
    )
    forecast_context = Context.create(
        name=f"calendar_offset_forecast_context_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )

    # Positive offset with FORECAST_POINT: FORECAST_POINT on Dec 27, offset=2 -> looks up Dec 25
    df_fp_pos = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-12-20 10:00:00"),
            "FORECAST_POINT": "2001|12|27",
            "üser id": 1,
        }
    ])
    obs_table_fp_pos = create_observation_table_by_upload(
        df_fp_pos, context_name=forecast_context.name
    )
    df_fp_pos_features = feature_list_pos.compute_historical_feature_table(
        obs_table_fp_pos, str(ObjectId())
    ).to_pandas()
    assert df_fp_pos_features[feature_name_pos].iloc[0] == "Christmas Day"

    # Negative offset with FORECAST_POINT: FORECAST_POINT on Dec 23, offset=-2 -> looks up Dec 25
    df_fp_neg = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-12-20 10:00:00"),
            "FORECAST_POINT": "2001|12|23",
            "üser id": 1,
        }
    ])
    obs_table_fp_neg = create_observation_table_by_upload(
        df_fp_neg, context_name=forecast_context.name
    )
    df_fp_neg_features = feature_list_neg.compute_historical_feature_table(
        obs_table_fp_neg, str(ObjectId())
    ).to_pandas()
    assert df_fp_neg_features[feature_name_neg].iloc[0] == "Christmas Day"


def test_calendar_lookup_feature_different_users(calendar_table):
    """
    Test that calendar lookup features respect per-user holiday data.

    All users observe New Year's Day; only odd user IDs observe Christmas Day.

    This verifies that the lookup correctly returns different values for different users
    on the same date, based on the per-user calendar data.
    """
    view = calendar_table.get_view()
    feature_name = f"CalendarHolidayNameByUser_{ObjectId()}"
    feature = view["public_holiday_name"].as_feature(feature_name)
    feature_list = FeatureList([feature], str(ObjectId()))

    # Use slightly different hours so all POINT_IN_TIME values are unique (for stable sort)
    preview_params = pd.DataFrame([
        # Both users observe New Year's Day
        {"POINT_IN_TIME": pd.Timestamp("2001-01-01 10:00:00"), "üser id": 1},
        {"POINT_IN_TIME": pd.Timestamp("2001-01-01 11:00:00"), "üser id": 2},
        # User 1 (odd): Christmas Day is a holiday
        {"POINT_IN_TIME": pd.Timestamp("2001-12-25 10:00:00"), "üser id": 1},
        # User 2 (even): Christmas Day is NOT a holiday
        {"POINT_IN_TIME": pd.Timestamp("2001-12-25 11:00:00"), "üser id": 2},
    ])
    expected = preview_params.copy()
    expected[feature_name] = ["New Year's Day", "New Year's Day", "Christmas Day", np.nan]
    obs_table = create_observation_table_by_upload(preview_params)
    df_features = feature_list.compute_historical_feature_table(
        obs_table, str(ObjectId())
    ).to_pandas()
    fb_assert_frame_equal(df_features, expected, sort_by_columns=["POINT_IN_TIME"])


def test_time_series_view_join_calendar_view_same_date_col_name(
    time_series_table_with_date_col,
    calendar_table_with_date_col,
    calendar_table_with_date_col_2,
):
    """
    Reproduce the SQL ambiguous column name error for both 'date' and 'tz_offset'.

    Root cause: apply_snapshots_datetime_transform uses unqualified column references
    (quoted_identifier(column_name)). When the left_node.sql already contains a JOIN where
    both L and R expose the same column name, those unqualified references become ambiguous.

    Both the time series table and the first calendar table have a 'tz_offset' column, so
    after step 2, both L and R in the FROM clause expose 'date' and 'tz_offset'.
    Step 3 then triggers ambiguity for both columns:
    - 'date' in DATE_TRUNC('day', "date")
    - 'tz_offset' in the timezone conversion (CONVERT_TIMEZONE(..., "tz_offset", ...))
    """
    time_series_view = time_series_table_with_date_col.get_view()
    cal1_view = calendar_table_with_date_col.get_view()
    cal2_view = calendar_table_with_date_col_2.get_view()

    # Step 2: first calendar join — produces SQL with both L."date" and R."date" in FROM clause
    ts_cal1_view = time_series_view.join(cal1_view, rprefix="cal1_")

    # Step 3: second calendar join — apply_snapshots_datetime_transform appends
    # DATE_TRUNC('day', "date") to the step-2 SQL, where "date" is ambiguous between L and R
    final_view = ts_cal1_view.join(cal2_view, rprefix="cal2_")
    df = final_view.preview(limit=10)
    assert len(df) > 0
    assert "date" in df.columns
    assert "cal2_date" in df.columns


def test_calendar_lookup_target(calendar_table):
    """
    Test creating a lookup target from CalendarView.
    """
    view = calendar_table.get_view()
    target = view["public_holiday_name"].as_target("CalendarHolidayTarget", fill_value=None)

    preview_params = [
        {"POINT_IN_TIME": "2001-01-01 10:00:00", "üser id": 1},
        {"POINT_IN_TIME": "2001-06-15 12:00:00", "üser id": 1},
        {"POINT_IN_TIME": "2001-12-25 08:00:00", "üser id": 1},
    ]
    df_preview = target.preview(pd.DataFrame(preview_params))

    holiday_values = df_preview.set_index("POINT_IN_TIME")["CalendarHolidayTarget"]

    assert holiday_values["2001-01-01 10:00:00"] == "New Year's Day"
    assert pd.isna(holiday_values["2001-06-15 12:00:00"])
    assert holiday_values["2001-12-25 08:00:00"] == "Christmas Day"
