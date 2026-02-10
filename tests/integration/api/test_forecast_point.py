"""
Integration tests for the forecast point related features
"""

import pandas as pd
from bson import ObjectId

from featurebyte import Context, FeatureJobSetting, FeatureList
from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.query_graph.model.forecast_point_schema import ForecastPointSchema
from tests.util.helper import check_preview_and_compute_historical_features


def _create_forecast_context(user_entity, format_string, suffix):
    """Create a forecast context for integration tests."""
    forecast_schema = ForecastPointSchema(
        granularity=TimeIntervalUnit.DAY,
        dtype=DBVarType.VARCHAR,
        is_utc_time=False,
        timezone="America/New_York",
        format_string=format_string,
    )
    return Context.create(
        name=f"forecast_context_{suffix}_{ObjectId()}",
        primary_entity=[user_entity.name],
        forecast_point_schema=forecast_schema,
    )


def test_forecast_point_hour_feature(user_entity, timestamp_format_string):
    """Test Context.get_forecast_point_feature().dt.hour via preview & historical feature table."""
    forecast_context = _create_forecast_context(
        user_entity=user_entity, format_string=timestamp_format_string, suffix="hour"
    )

    feature = forecast_context.get_forecast_point_feature().dt.hour
    feature.name = "forecast_context_hour"

    feature_list = FeatureList([feature], "test_forecast_context_hour_feature_list")
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "FORECAST_POINT": "2001|01|10",
        }
    ])
    expected = preview_params.copy()
    expected["forecast_context_hour"] = [0]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)


def test_days_until_forecast_from_latest_event(event_table, timestamp_format_string, user_entity):
    """Test forecast point minus latest event timestamp via preview & historical feature table."""
    forecast_context = _create_forecast_context(
        user_entity=user_entity, format_string=timestamp_format_string, suffix="days_until"
    )

    view = event_table.get_view()
    latest_event_timestamp_feature = view.groupby("ÜSER ID").aggregate_over(
        value_column="ËVENT_TIMESTAMP",
        method="latest",
        windows=["90d"],
        feature_names=["latest_event_timestamp_90d"],
        feature_job_setting=FeatureJobSetting(blind_spot="30m", period="1h", offset="30m"),
    )["latest_event_timestamp_90d"]

    feature = (
        forecast_context.get_forecast_point_feature() - latest_event_timestamp_feature
    ).dt.day
    feature.name = "Days Until Forecast (from latest event)"

    feature_list = FeatureList([feature], "test_days_until_forecast_feature_list")
    preview_params = pd.DataFrame([
        {
            "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
            "üser id": 1,
            "FORECAST_POINT": "2001|01|10",
        }
    ])
    expected = preview_params.copy()
    expected["Days Until Forecast (from latest event)"] = [7.845613]
    check_preview_and_compute_historical_features(feature_list, preview_params, expected)
