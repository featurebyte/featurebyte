"""
Test view class
"""
from typing import Dict, List, Tuple

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
from featurebyte.query_graph.enum import NodeOutputType, NodeType


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


def test_update_metadata(simple_test_view):
    """
    Test update metadata
    """
    # stub out some new values we want to update
    new_node_name = "new_node_name"
    new_cols_info = [ColumnInfo(name="colB", dtype=DBVarType.FLOAT)]
    new_col_lineage_map = {"colX": ("a", "b")}
    new_joined_data_ids = [get_random_pydantic_object_id()]

    # verify that the initial state is not the updated state
    assert simple_test_view.node_name != new_node_name
    assert simple_test_view.columns_info != new_cols_info
    assert simple_test_view.column_lineage_map != new_col_lineage_map
    assert simple_test_view.row_index_lineage != ("new_node_name",)
    assert simple_test_view.tabular_data_ids != new_joined_data_ids

    # update state
    simple_test_view.update_metadata(
        new_node_name, new_cols_info, new_col_lineage_map, new_joined_data_ids
    )

    # verify that state is updated
    assert simple_test_view.node_name == new_node_name
    assert simple_test_view.columns_info == new_cols_info
    assert simple_test_view.column_lineage_map == new_col_lineage_map
    assert simple_test_view.row_index_lineage == ("new_node_name",)
    assert simple_test_view.tabular_data_ids == new_joined_data_ids


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
    col_to_use = "join_col"
    current_view = SimpleTestView(join_col=col_to_use)
    other_view = SimpleTestView(join_col=col_to_use)

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


def test_join__left_join():
    """
    Test left join.
    """
    # setup
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    current_view = SimpleTestView(
        columns_info=[col_info_a, col_info_b],
    )
    other_view = SimpleTestView(columns_info=[col_info_c])
    input_node = current_view.graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "generic",
            "columns": ["random_column"],
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    current_view.node_name = input_node.name

    input_node = other_view.graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "generic",
            "columns": ["random_column"],
            "table_details": {
                "database_name": "db",
                "schema_name": "public",
                "table_name": "transaction",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "account": "sf_account",
                    "warehouse": "sf_warehouse",
                    "database": "db",
                    "sf_schema": "public",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    other_view.node_name = input_node.name

    # do the join
    current_view.join(other_view, on="join_col")

    # assert
    current_view.columns_info = [col_info_a, col_info_b, col_info_c]
    # TODO: finish asserting more stuff
