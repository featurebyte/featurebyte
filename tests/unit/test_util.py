"""
Test helper functions in featurebyte/util.py
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.exception import RecordRetrievalException
from featurebyte.util import get_entity, get_event_data


def test_get_entity():
    """
    Test get_entity function
    """
    # create entities & save to persistent
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    prod_entity = Entity(name="product", serving_names=["prod_id"])
    region_entity = Entity(name="region", serving_names=["region"])
    cust_entity.save()
    prod_entity.save()
    region_entity.save()

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
    snowflake_event_data.save()

    # load the event data from the persistent
    loaded_event_data = get_event_data(snowflake_event_data.name)
    assert loaded_event_data == snowflake_event_data

    with pytest.raises(RecordRetrievalException) as exc:
        get_event_data("unknown_event_data")
    assert 'EventData name (event_data.name: "unknown_event_data") not found!' in str(exc.value)
