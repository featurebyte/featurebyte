"""
Test series validator module
"""
from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

import pytest
from bson import ObjectId

from featurebyte.core.series_validator import _are_series_both_of_type, _validate_entity_ids
from featurebyte.enum import TableDataType
from featurebyte.models.base import PydanticObjectId

if TYPE_CHECKING:
    from featurebyte.core.series import Series

    SeriesT = TypeVar("SeriesT", bound=Series)


def test_validate_entity_ids():
    """
    Test _validate_entity_ids
    """
    # no IDs should error
    with pytest.raises(ValueError) as exc_info:
        _validate_entity_ids([])
    assert "no, or multiple, entity IDs found" in str(exc_info)

    # >1 ID should error
    object_id_1 = PydanticObjectId(ObjectId())
    object_id_2 = PydanticObjectId(ObjectId())
    with pytest.raises(ValueError) as exc_info:
        _validate_entity_ids([object_id_1, object_id_2])
    assert "no, or multiple, entity IDs found" in str(exc_info)

    # exactly one ID shouldn't error
    _validate_entity_ids([object_id_1])


def test_validate_entity():
    """
    Test _validate_entity
    """
    # TODO
    pass


def test_are_series_both_of_type(int_series):
    """
    Test _are_series_both_of_type
    """
    # they are not of type SCD_DATA because the nodes do not have the right params
    assert not _are_series_both_of_type(int_series, int_series, TableDataType.SCD_DATA)

    # TODO


def test_is_from_same_data():
    """
    Test _is_from_same_data
    """
    # TODO
    pass


def test_is_series_a_lookup_feature():
    """
    Test _is_series_a_lookup_feature
    """
    # TODO
    pass


def test_both_are_lookup_features():
    """
    Test _both_are_lookup_features
    """
    # TODO
    pass


def test_get_event_and_item_data():
    """
    Test _get_event_and_item_data
    """
    # TODO
    pass


def test_is_one_item_and_one_event():
    """
    Test _is_one_item_and_one_event
    """
    # TODO
    pass


def test_item_data_and_event_data_are_related():
    """
    Test _item_data_and_event_data_are_related
    """
    # TODO
    pass


def test_validate_feature_type():
    """
    Test _validate_feature_type
    """
    # TODO
    pass


def test_validate_series():
    """
    Test validate_series
    """
    # TODO
    pass
