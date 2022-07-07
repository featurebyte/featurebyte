import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte import errors
from featurebyte.query_graph.feature_historical import (
    get_historical_features,
    get_historical_features_sql,
)


def test_get_historical_features__missing_point_in_time(mock_snowflake_feature):
    training_events = pd.DataFrame(
        {
            "CUST_ID": ["C1", "C2", "C3"],
        }
    )
    with pytest.raises(errors.MissingPointInTimeColumnError) as exc_info:
        get_historical_features(
            feature_objects=[mock_snowflake_feature], training_events=training_events
        )
    assert str(exc_info.value) == "POINT_IN_TIME column is required"


@freeze_time("2022-05-01")
@pytest.mark.parametrize("point_in_time_is_datetime_dtype", [True, False])
def test_get_historical_features__too_recent_point_in_time(
    mock_snowflake_feature, point_in_time_is_datetime_dtype
):

    point_in_time_vals = ["2022-04-15", "2022-04-30"]
    if point_in_time_is_datetime_dtype:
        point_in_time_vals = pd.to_datetime(point_in_time_vals)
    training_events = pd.DataFrame(
        {
            "POINT_IN_TIME": point_in_time_vals,
            "CUST_ID": ["C1", "C2"],
        }
    )
    with pytest.raises(errors.TooRecentPointInTimeError) as exc_info:
        get_historical_features(
            feature_objects=[mock_snowflake_feature], training_events=training_events
        )
    assert str(exc_info.value) == (
        "The latest point in time (2022-04-30 00:00:00) should not be more recent than 48 hours "
        "from now"
    )


def test_get_historical_feature_sql(float_feature):
    feature_objects = [float_feature]
    request_table_columns = ["POINT_IN_TIME", "CUST_ID", "A", "B", "C"]
    sql = get_historical_features_sql(
        feature_objects=feature_objects, request_table_columns=request_table_columns
    )
    update_fixtures = False
    if update_fixtures:
        with open(
            "tests/fixtures/expected_historical_requests.sql", "w", encoding="utf-8"
        ) as f_handle:
            f_handle.write(sql)
            raise AssertionError("Fixture updated, please set update_fixture to False")
    with open("tests/fixtures/expected_historical_requests.sql", encoding="utf-8") as f_handle:
        expected_feature_sql = f_handle.read()
    assert sql.strip() == expected_feature_sql.strip()
