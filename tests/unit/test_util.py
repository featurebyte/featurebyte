"""
Test helper functions in featurebyte/util.py
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.exception import RecordRetrievalException
from featurebyte.util import get_entity, get_event_data


def test_get_entity(mock_get_persistent):
    """
    Test get_entity function
    """
    _ = mock_get_persistent

    # create entities & save to persistent
    cust_entity = Entity.create(name="customer", serving_name="cust_id")
    prod_entity = Entity.create(name="product", serving_name="prod_id")
    region_entity = Entity.create(name="region", serving_name="region")

    # load the entities from the persistent
    assert get_entity("customer") == cust_entity
    assert get_entity("product") == prod_entity
    assert get_entity("region") == region_entity


def test_get_event_data(snowflake_event_data, mock_config_path_env):
    """
    Test get_event_data function
    """
    _ = mock_config_path_env

    # create event data & save to persistent
    snowflake_event_data.save_as_draft()

    # load the event data from the persistent
    loaded_event_data = get_event_data(snowflake_event_data.name)
    assert loaded_event_data == snowflake_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        get_event_data("unknown_event_data")
    assert 'Event data name "unknown_event_data" not found!' in str(exc.value)
