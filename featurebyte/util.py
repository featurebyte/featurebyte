"""
Helper functions to construct different user facing objects
"""
from __future__ import annotations

from http import HTTPStatus

from featurebyte.api.event_data import EventData
from featurebyte.config import Configurations
from featurebyte.exception import RecordRetrievalException


def get_event_data(event_data_name: str) -> EventData:
    """
    Retrieve event data from the persistent given event data name

    Parameters
    ----------
    event_data_name: str
        Event data name

    Returns
    -------
    EventData
        EventData object of the given event data name

    Raises
    ------
    RecordRetrievalException
        When the event data not found
    """
    client = Configurations().get_client()
    response = client.get(url="/event_data/", params={"name": event_data_name})
    if response.status_code == HTTPStatus.OK:
        response_dict = response.json()
        if response_dict["data"]:
            event_data_dict = response_dict["data"][0]
            return EventData(**event_data_dict)
    raise RecordRetrievalException(
        response, f'EventData name (event_data.name: "{event_data_name}") not found!'
    )
