"""
Unit tests for ObservationView class
"""
import pytest

from featurebyte.api.observation_view import ObservationView


@pytest.mark.parametrize(
    "table_name",
    [
        "snowflake_event_view",
        "snowflake_item_view",
        "snowflake_scd_view",
        "snowflake_dimension_view",
    ],
)
def test_get_observation_view_from_view(request, table_name):
    """
    Test ObservationView can be created from different views
    """
    view = request.getfixturevalue(table_name)
    observation_view = view.get_observation_view()

    assert isinstance(observation_view, ObservationView)
    assert observation_view.columns == view.columns

    if view.timestamp_column is None:
        assert observation_view.point_in_time_column is None
    else:
        assert observation_view.point_in_time_column == view.timestamp_column
