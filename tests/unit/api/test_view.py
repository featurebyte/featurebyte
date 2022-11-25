"""
Test view class
"""
from typing import Dict, List, Tuple

from unittest.mock import patch

import pytest
from bson import ObjectId
from pydantic import StrictStr

from featurebyte.api.view import View, ViewColumn
from featurebyte.enum import DBVarType
from featurebyte.exception import NoJoinKeyFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import (
    ColumnInfo,
    FeatureStoreModel,
    TableDetails,
    TabularSource,
)


class SimpleTestViewColumn(ViewColumn):
    """
    SimpleTestViewColumn class
    """


class SimpleTestView(View):
    """
    Simple view that can be used, and configured for tests.
    """

    _series_class = SimpleTestViewColumn

    columns_info: List[ColumnInfo] = []
    node_name = "random_node"
    row_index_lineage: Tuple[StrictStr, ...] = ()
    tabular_data_ids: List[PydanticObjectId] = []
    tabular_source: TabularSource = TabularSource(
        feature_store_id=PydanticObjectId(ObjectId("6332f9651050ee7d1234660d")),
        table_details=TableDetails(table_name="table"),
    )
    feature_store: FeatureStoreModel = None
    column_lineage_map: Dict[str, Tuple[str, ...]] = {}

    join_col = ""

    def protected_attributes(self) -> list[str]:
        return []

    def get_join_column(self) -> str:
        return self.join_col

    def set_join_col_override(self, join_col_override: str):
        """
        Test helper to set the join column override.
        """
        self.join_col = join_col_override


@pytest.fixture(name="simple_test_view")
def get_test_view_fixture():
    """
    Get a test view fixture
    """
    return SimpleTestView()


def test_update_metadata():
    """
    Test update metadata
    """
    pass


def get_random_pydantic_object_id() -> PydanticObjectId:
    """
    Helper function to return a random pydantic object ID.
    """
    return PydanticObjectId(ObjectId())


def test_check_key_is_entity_in_view(simple_test_view):
    """
    Test check_key_is_entity_in_view
    """
    in_view = View.check_key_is_entity_in_view(simple_test_view, "random_column")
    assert not in_view

    col_name = "colA"
    simple_test_view.columns_info = [
        ColumnInfo(name="colA", dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
    ]
    in_view = View.check_key_is_entity_in_view(simple_test_view, col_name)
    assert in_view


def test_check_if_key_is_entity_in_both():
    """
    Test check_if_key_is_entity_in_both
    """
    view_a = SimpleTestView()
    view_b = SimpleTestView()
    col_name = "colA"
    # Verify that views with no entities returns false
    key_is_entity = view_a.check_if_key_is_entity_in_both(view_b, col_name)
    assert not key_is_entity

    # Verify that one view with entity, and other without, returns false
    view_a.columns_info = [
        ColumnInfo(name=col_name, dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
    ]
    key_is_entity = view_a.check_if_key_is_entity_in_both(view_b, col_name)
    assert not key_is_entity

    # Verify that both views with entities, returns true
    view_b.columns_info = [
        ColumnInfo(name=col_name, dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
    ]
    key_is_entity = view_a.check_if_key_is_entity_in_both(view_b, col_name)
    assert key_is_entity


def test_get_join_keys__on_col_provided():
    """
    Test get_join_keys where on override is provided
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()
    col_name = "col_to_use"
    left_join_key, right_join_key = current_view.get_join_keys(other_view, on_column=col_name)
    assert left_join_key == right_join_key
    assert left_join_key == col_name


def test_get_join_keys__same_join_keys():
    """
    Test get_join_keys where join keys are the same
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    col_to_use = "join_col"
    current_view.join_col = col_to_use
    other_view.join_col = col_to_use

    left_join_key, right_join_key = current_view.get_join_keys(other_view)
    assert left_join_key == right_join_key
    assert left_join_key == col_to_use


def test_get_join_keys__is_entity():
    """
    Test get_join_keys where keys are entities
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    col_name = "join_col"
    current_view.columns_info = [
        ColumnInfo(name=col_name, dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
    ]
    other_view.columns_info = [
        ColumnInfo(name=col_name, dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
    ]
    # Set the join col on one of them, but not on the other
    current_view.join_col = col_name

    left_join_key, right_join_key = current_view.get_join_keys(other_view)
    assert left_join_key == right_join_key
    assert left_join_key == col_name


def test_get_join_keys__error_if_no_key_found():
    """
    Test get_join_keys errors if no key is found
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    with pytest.raises(NoJoinKeyFoundError):
        current_view.get_join_keys(other_view)


def test_join():
    """
    Test join
    """
    pass
