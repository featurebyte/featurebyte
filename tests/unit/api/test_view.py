"""
Test view class
"""

from typing import Any, ClassVar, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId

from featurebyte.api.view import View, ViewColumn
from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import NoJoinKeyFoundError, RepeatedColumnNamesError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import SnowflakeDetails, TableDetails
from tests.util.helper import get_node, reset_global_graph


class SimpleTestViewColumn(ViewColumn):
    """
    SimpleTestViewColumn class
    """


class SimpleTestView(View):
    """
    Simple view that can be used, and configured for tests.
    """

    _series_class: ClassVar[Any] = SimpleTestViewColumn

    columns_info: List[ColumnInfo] = []
    node_name: str = "random_node"
    tabular_source: TabularSource = TabularSource(
        feature_store_id=PydanticObjectId(ObjectId("6332f9651050ee7d1234660d")),
        table_details=TableDetails(table_name="table"),
    )
    feature_store: FeatureStoreModel = FeatureStoreModel(
        name="random_featurestore",
        type=SourceType.TEST,
        details=SnowflakeDetails(
            account="sf_account",
            database_name="sf_database",
            role_name="TESTING",
            schema_name="sf_schema",
            warehouse="sf_warehouse",
        ),
    )

    join_col: str = ""
    excluded_columns_override: Optional[List[str]] = None

    @property
    def node(self):
        if self.node_name == "random_node":
            node = MagicMock()
            node.name = "random_node"
            return node
        return super().node

    def protected_attributes(self) -> List[str]:
        return []

    def _get_join_column(self) -> Optional[str]:
        return self.join_col

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        if "exclude" in kwargs:
            return {}
        return super().model_dump(**kwargs)

    def set_join_col_override(self, join_col_override: str):
        """
        Test helper to set the join column override.
        """
        self.join_col = join_col_override

    def _get_additional_excluded_columns_as_other_view(self):
        if self.excluded_columns_override is None:
            return super()._get_additional_excluded_columns_as_other_view()
        return self.excluded_columns_override


@pytest.fixture(name="simple_test_view")
def get_test_view_fixture():
    """
    Get a test view fixture
    """
    return SimpleTestView()


def test_create_joined_view(simple_test_view):
    """
    Test update metadata
    """
    # stub out some new values we want to update
    new_node_name = "new_node_name"
    new_cols_info = [ColumnInfo(name="colB", dtype=DBVarType.FLOAT)]

    # verify that the initial state is not the updated state
    assert simple_test_view.node_name != new_node_name
    assert simple_test_view.columns_info != new_cols_info

    # update state
    joined_view = simple_test_view._create_joined_view(new_node_name, new_cols_info)

    # verify that state is updated
    assert joined_view.node_name == new_node_name
    assert joined_view.columns_info == new_cols_info


def get_random_pydantic_object_id() -> PydanticObjectId:
    """
    Helper function to return a random pydantic object ID.
    """
    return PydanticObjectId(ObjectId())


def test_get_key_if_entity__other_view_is_not_entity():
    """
    Test get_key_if_entity__other_view_is_not_entity
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView(columns_info=[ColumnInfo(name="colA", dtype=DBVarType.INT)])
    response = current_view._get_key_if_entity(other_view)
    assert response is None


def test_get_key_if_entity__diff_entities_in_both_is_no_match():
    """
    Test get_key_if_entity__diff_entities_in_both_is_no_match
    """
    current_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colA", dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
        ]
    )
    other_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colB", dtype=DBVarType.INT, entity_id=get_random_pydantic_object_id())
        ]
    )
    other_view.set_join_col_override("colB")
    response = current_view._get_key_if_entity(other_view)
    assert response is None


def test_get_key_if_entity__same_entity_in_both_is_match():
    """
    Test get_key_if_entity__same_entity_in_both_is_match
    """
    entity_id = get_random_pydantic_object_id()
    current_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colA", dtype=DBVarType.INT),
            ColumnInfo(name="colB", dtype=DBVarType.INT, entity_id=entity_id),
            ColumnInfo(name="colC", dtype=DBVarType.INT),
        ]
    )
    other_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colD", dtype=DBVarType.INT),
            ColumnInfo(name="colE", dtype=DBVarType.INT, entity_id=entity_id),
            ColumnInfo(name="colF", dtype=DBVarType.INT),
        ]
    )
    other_view.set_join_col_override("colE")
    left, right = current_view._get_key_if_entity(other_view)
    assert left == "colB"
    assert right == "colE"


def test_get_key_if_entity__multiple_entity_is_no_match():
    """
    Test get_key_if_entity__multiple_entity_is_no_match
    """
    entity_id = get_random_pydantic_object_id()
    current_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colA", dtype=DBVarType.INT),
            ColumnInfo(name="colB", dtype=DBVarType.INT, entity_id=entity_id),
            ColumnInfo(name="colC", dtype=DBVarType.INT, entity_id=entity_id),
        ]
    )
    other_view = SimpleTestView(
        columns_info=[
            ColumnInfo(name="colD", dtype=DBVarType.INT),
            ColumnInfo(name="colE", dtype=DBVarType.INT, entity_id=entity_id),
            ColumnInfo(name="colF", dtype=DBVarType.INT),
        ]
    )
    other_view.set_join_col_override("colE")
    response = current_view._get_key_if_entity(other_view)
    assert response is None


def test_get_join_keys__empty_string_on_should_not_be_used():
    """
    Test get_join_keys - empty `on` string should raise an error
    """
    col_info_a = ColumnInfo(name="colA", dtype=DBVarType.INT)
    current_view = SimpleTestView(columns_info=[col_info_a])
    other_view = SimpleTestView(join_col=col_info_a.name)
    on_col = ""
    with pytest.raises(ValueError) as exc_info:
        current_view._get_join_keys(other_view, on_column=on_col)
    assert "The `on` column should not be empty." in str(exc_info)


def test_get_join_keys__on_col_provided():
    """
    Test get_join_keys where on override is provided
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()
    other_view_join_col = "join_col"
    other_view.set_join_col_override(other_view_join_col)
    col_name = "col_to_use"
    left_join_key, right_join_key = current_view._get_join_keys(other_view, on_column=col_name)
    assert right_join_key == other_view_join_col
    assert left_join_key == col_name


def test_get_join_keys__target_join_key_is_column_in_calling_view():
    """
    Test get_join_keys where join key of target view is a column in the calling view.
    """
    col_to_use = "colB"
    current_view = SimpleTestView(join_col=col_to_use)
    current_view.columns_info = [
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    ]
    other_view = SimpleTestView(join_col=col_to_use)

    left_join_key, right_join_key = current_view._get_join_keys(other_view)
    assert left_join_key == right_join_key
    assert left_join_key == col_to_use


def test_get_join_keys__is_entity():
    """
    Test get_join_keys where keys are entities
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    entity_id = get_random_pydantic_object_id()
    current_view.columns_info = [ColumnInfo(name="colA", dtype=DBVarType.INT, entity_id=entity_id)]
    other_view.columns_info = [ColumnInfo(name="colB", dtype=DBVarType.INT, entity_id=entity_id)]
    # Set the join col on one of them, but not on the other
    other_view.set_join_col_override("colB")

    left_join_key, right_join_key = current_view._get_join_keys(other_view)
    assert left_join_key == "colA"
    assert right_join_key == "colB"


def test_get_join_keys__prefer_entity_over_matching_cols():
    """
    Test that entity matching is preferred over matching columns
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    entity_id = get_random_pydantic_object_id()
    current_view.columns_info = [
        ColumnInfo(name="colC", dtype=DBVarType.INT),
        ColumnInfo(name="colA", dtype=DBVarType.INT, entity_id=entity_id),
    ]
    other_view.columns_info = [
        ColumnInfo(name="colC", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT, entity_id=entity_id),
    ]
    # Set the join col on one of them, but not on the other
    other_view.set_join_col_override("colB")

    left_join_key, right_join_key = current_view._get_join_keys(other_view)
    assert left_join_key == "colA"
    assert right_join_key == "colB"


def test_get_join_keys__error_if_no_key_found():
    """
    Test get_join_keys errors if no key is found
    """
    current_view = SimpleTestView()
    other_view = SimpleTestView()

    with pytest.raises(NoJoinKeyFoundError):
        current_view._get_join_keys(other_view)


@pytest.fixture(name="generic_input_node_params")
def get_generic_input_node_params_fixture():
    node_params = {
        "type": "source_table",
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
                "database_name": "db",
                "schema_name": "public",
                "role_name": "role",
            },
        },
    }
    return {
        "node_type": NodeType.INPUT,
        "node_params": node_params,
        "node_output_type": NodeOutputType.FRAME,
        "input_nodes": [],
    }


@pytest.mark.parametrize("join_type_param", ["left", "inner"])
def test_join__left_join(generic_input_node_params, join_type_param):
    """
    Test left and inner join.
    """
    # reset between tests
    reset_global_graph()
    # setup
    col_info_a, col_info_b, col_info_c, col_info_d, col_info_e = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
        ColumnInfo(name="colD", dtype=DBVarType.INT),
        ColumnInfo(name="colE", dtype=DBVarType.INT),
    )
    current_view = SimpleTestView(
        columns_info=[col_info_a, col_info_b],
    )
    other_view = SimpleTestView(columns_info=[col_info_c, col_info_d, col_info_e], join_col="colC")
    generic_input_node_params["node_params"]["columns"] = ["colA", "colB", "colC"]
    input_node = current_view.graph.add_operation(
        node_type=generic_input_node_params["node_type"],
        node_params=generic_input_node_params["node_params"],
        node_output_type=generic_input_node_params["node_output_type"],
        input_nodes=generic_input_node_params["input_nodes"],
    )
    current_view.node_name = input_node.name
    assert current_view.node_name == "input_1"
    assert current_view.columns_info == [col_info_a, col_info_b]

    generic_input_node_params["node_params"]["columns"] = ["colC", "colD", "colE"]
    input_node = other_view.graph.add_operation(
        node_type=generic_input_node_params["node_type"],
        node_params=generic_input_node_params["node_params"],
        node_output_type=generic_input_node_params["node_output_type"],
        input_nodes=generic_input_node_params["input_nodes"],
    )
    other_view.node_name = input_node.name
    assert other_view.node_name == "input_2"
    assert other_view.columns_info == [col_info_c, col_info_d, col_info_e]

    # do the join
    joined_view = current_view.join(
        other_view, on=col_info_a.name, how=join_type_param, rsuffix="suffix", rprefix="_"
    )

    # test sample table node (take the left input)
    sample_table_node = joined_view.graph.get_sample_table_node(joined_view.node_name)
    assert sample_table_node.name == "input_1"

    # assert updated view params
    assert joined_view.columns_info == [
        col_info_a,
        col_info_b,
        ColumnInfo(name="_colDsuffix", dtype=DBVarType.INT),
        ColumnInfo(name="_colEsuffix", dtype=DBVarType.INT),
    ]
    assert joined_view.node_name == "join_1"

    # assert graph node
    view_dict = joined_view.model_dump()
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict == {
        "name": "join_1",
        "output_type": "frame",
        "parameters": {
            "join_type": join_type_param,
            "left_input_columns": ["colA", "colB"],
            "left_on": "colA",
            "left_output_columns": ["colA", "colB"],
            "right_input_columns": ["colD", "colE"],
            "right_on": "colC",
            "right_output_columns": ["_colDsuffix", "_colEsuffix"],
            "scd_parameters": None,
            "metadata": {"type": "join", "rsuffix": "suffix", "rprefix": "_"},
        },
        "type": "join",
    }
    assert view_dict["graph"]["edges"] == [
        {"source": "input_2", "target": "join_1"},
        {"source": "input_1", "target": "join_1"},
    ]


@pytest.fixture(name="patch_graph_operations")
def patch_graph_operations_fixture():
    """
    Patch graph related operations unrelated to join validation logic
    """
    mocked_new_node = MagicMock()
    mocked_new_node.name = "new_node"
    with patch(
        "featurebyte.query_graph.graph.GlobalQueryGraph.add_operation", return_value=mocked_new_node
    ):
        yield


@pytest.mark.usefixtures("patch_graph_operations")
def test_validate_join__no_overlapping_columns():
    """
    Test validate join helper method
    """
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(columns_info=[col_info_a, col_info_b], join_col=col_info_b.name)
    view_without_overlap = SimpleTestView(columns_info=[col_info_c], join_col=col_info_c.name)

    # joining two views with no overlapping columns should have an error
    with pytest.raises(NoJoinKeyFoundError) as exc_info:
        base_view.join(view_without_overlap)
    assert "Unable to automatically find a default join column key" in str(exc_info)

    # no overlap should have no error with suffix, since we'll use primary keys to join
    base_view.join(view_without_overlap, on="colA", rsuffix="suffix")


@pytest.mark.usefixtures("patch_graph_operations")
def test_validate_join__one_overlapping_column():
    """
    Test validate join helper method
    """
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(columns_info=[col_info_a, col_info_b], join_col=col_info_b.name)
    view_with_overlap = SimpleTestView(
        columns_info=[col_info_b, col_info_c], join_col=col_info_b.name
    )
    view_with_overlap_not_join_key = SimpleTestView(
        columns_info=[col_info_a, col_info_c], join_col=col_info_c.name
    )

    # overlapping column names here should have error since the overlapping name is not the join key
    with pytest.raises(NoJoinKeyFoundError):
        base_view.join(view_with_overlap_not_join_key)

    # overlapping column names here should have no error since the overlapping names are the join keys
    base_view.join(view_with_overlap)

    # no overlap should have no error with suffix and overlap
    base_view.join(view_with_overlap, rsuffix="suffix")


@pytest.mark.usefixtures("patch_graph_operations")
def test_validate_join__multiple_overlapping_columns():
    """
    Test validate join helper method
    """
    col_info_a, col_info_b, col_info_c, col_info_d = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
        ColumnInfo(name="colD", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(
        columns_info=[col_info_a, col_info_b, col_info_d], join_col=col_info_b.name
    )
    view_with_multiple_overlapping = SimpleTestView(
        columns_info=[col_info_a, col_info_b, col_info_c, col_info_d], join_col=col_info_b.name
    )

    # multiple overlapping column names should throw an error if no suffix is provided
    with pytest.raises(RepeatedColumnNamesError) as exc_info:
        base_view.join(view_with_multiple_overlapping)
    assert "Duplicate column names ['colA', 'colD'] found" in str(exc_info.value)

    # multiple overlapping column names should not throw an error if suffix is provided
    base_view.join(view_with_multiple_overlapping, rsuffix="suffix")

    # also ok if prefix is provided
    base_view.join(view_with_multiple_overlapping, rprefix="prefix")


@pytest.mark.usefixtures("patch_graph_operations")
def test_validate_join__additional_excluded_columns():
    """
    Test join validation with additional excluded columns
    """
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(columns_info=[col_info_a, col_info_b, col_info_c])
    other_view = SimpleTestView(
        columns_info=[col_info_a, col_info_b, col_info_c],
        join_col=col_info_b.name,
        excluded_columns_override=["colA", "colC"],
    )
    # should have no error since the repeated columns are excluded from join result
    base_view.join(other_view)


def test_validate_join__repeated_caused_by_modifiers():
    """
    Test the case where repeated column names are caused by modifiers
    """
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    col_info_a_short, col_info_b_short = (
        ColumnInfo(name="A", dtype=DBVarType.INT),
        ColumnInfo(name="B", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(columns_info=[col_info_a, col_info_b, col_info_c])
    other_view = SimpleTestView(
        columns_info=[col_info_a_short, col_info_b_short, col_info_c],
        join_col=col_info_b_short.name,
    )
    with pytest.raises(RepeatedColumnNamesError) as exc_info:
        base_view.join(other_view, rprefix="col", on="colB")
    assert "Duplicate column names ['colA'] found" in str(exc_info.value)


@pytest.mark.usefixtures("patch_graph_operations")
def test_validate_join__check_on_column():
    """
    Test validate join method for on column.
    """
    col_info_a, col_info_b, col_info_c = (
        ColumnInfo(name="colA", dtype=DBVarType.INT),
        ColumnInfo(name="colB", dtype=DBVarType.INT),
        ColumnInfo(name="colC", dtype=DBVarType.INT),
    )
    base_view = SimpleTestView(columns_info=[col_info_a, col_info_b], join_col=col_info_a.name)
    other_view = SimpleTestView(columns_info=[col_info_c], join_col=col_info_c.name)

    # `on` provided for column in calling view should have no error
    base_view.join(other_view, on=col_info_a.name, rsuffix="_suffix")

    # `on` provided for column not in calling view should have an error
    with pytest.raises(NoJoinKeyFoundError) as exc_info:
        base_view.join(other_view, on=col_info_c.name, rsuffix="_suffix")
    assert "The `on` column name provided 'colC' is not found in the calling view" in str(exc_info)
