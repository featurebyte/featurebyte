"""
Integration tests for ObservationView
"""


def test_observation_view(event_table):
    """
    Test ObservationView
    """
    event_view = event_table.get_view()

    columns = ["ËVENT_TIMESTAMP", "CUST_ID", "ÜSER ID"]
    observation_view = event_view.get_observation_view()
    observation_view = observation_view[columns]

    df = observation_view.preview()
    assert df.columns.tolist() == columns
