"""
Unit test for EventView class
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node


@pytest.fixture(name="snowflake_event_view")
def snowflake_event_view_fixture(snowflake_event_data, config):
    """
    EventData object fixture
    """
    _ = config
    snowflake_event_data.update_default_feature_job_setting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    event_view = EventView.from_event_data(event_data=snowflake_event_data)
    yield event_view


def test_from_event_data(snowflake_event_data, mock_get_persistent):
    """
    Test from_event_data
    """
    _ = mock_get_persistent
    event_view_first = EventView.from_event_data(snowflake_event_data)
    assert event_view_first.tabular_source == snowflake_event_data.tabular_source
    assert event_view_first.node == snowflake_event_data.node
    assert event_view_first.column_var_type_map == snowflake_event_data.column_var_type_map
    assert event_view_first.row_index_lineage == snowflake_event_data.row_index_lineage
    assert event_view_first.column_entity_map is snowflake_event_data.column_entity_map is None

    entity = Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_data.cust_id.as_entity("customer")
    snowflake_event_data.update_default_feature_job_setting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    event_view_second = EventView.from_event_data(snowflake_event_data)
    assert event_view_second.column_entity_map == snowflake_event_data.column_entity_map
    assert event_view_second.column_entity_map == {"cust_id": str(entity.id)}
    assert event_view_second.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )


def test_getitem__str(snowflake_event_view):
    """
    Test retrieving single column
    """
    cust_id = snowflake_event_view["cust_id"]
    assert isinstance(cust_id, Series)
    assert cust_id.node == Node(
        name="project_1",
        type=NodeType.PROJECT,
        parameters={"columns": ["cust_id"]},
        output_type=NodeOutputType.SERIES,
    )
    assert cust_id.lineage == ("input_2", "project_1")
    assert cust_id.row_index_lineage == ("input_2",)
    assert cust_id.parent.node.name == "input_2"


def test_getitem__list_of_str(snowflake_event_view):
    """
    Test retrieving subset of the event data features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_view_subset1 = snowflake_event_view[["col_float"]]
    assert isinstance(event_view_subset1, EventView)
    assert set(event_view_subset1.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset1.row_index_lineage == snowflake_event_view.row_index_lineage
    assert event_view_subset1.inception_node == snowflake_event_view.inception_node
    assert (
        event_view_subset1.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # case 2: select a non-protected column with a timestamp column
    event_view_subset2 = snowflake_event_view[["col_float", "event_timestamp"]]
    assert isinstance(event_view_subset2, EventView)
    assert set(event_view_subset2.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset2.row_index_lineage == snowflake_event_view.row_index_lineage
    assert event_view_subset2.inception_node == snowflake_event_view.inception_node
    assert (
        event_view_subset2.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # both event data subsets actually point to the same node
    assert event_view_subset1.node == event_view_subset2.node


def test_getitem__series_key(snowflake_event_view):
    """
    Test filtering on event data object
    """
    mask_cust_id = snowflake_event_view["cust_id"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.var_type == DBVarType.BOOL

    event_view_row_subset = snowflake_event_view[mask_cust_id]
    assert isinstance(event_view_row_subset, EventView)
    assert event_view_row_subset.row_index_lineage == ("input_2", "filter_1")
    assert event_view_row_subset.inception_node == snowflake_event_view.inception_node


@pytest.mark.parametrize("column", ["event_timestamp"])
def test_setitem__override_protected_column(snowflake_event_view, column):
    """
    Test attempting to change event data's timestamp value or entity identifier value
    """
    assert column in snowflake_event_view.protected_columns
    with pytest.raises(ValueError) as exc:
        snowflake_event_view[column] = 1
    expected_msg = f"Timestamp or entity column '{column}' cannot be modified!"
    assert expected_msg in str(exc.value)


def test_setitem__str_key_series_value(snowflake_event_view):
    """
    Test assigning Series object to event_view
    """
    double_value = snowflake_event_view["col_float"] * 2
    assert isinstance(double_value, Series)
    snowflake_event_view["double_value"] = double_value
    assert snowflake_event_view.node == Node(
        name="assign_1",
        type=NodeType.ASSIGN,
        parameters={"name": "double_value"},
        output_type=NodeOutputType.FRAME,
    )
    assert snowflake_event_view.column_lineage_map == {
        "col_binary": ("input_2",),
        "col_boolean": ("input_2",),
        "col_char": ("input_2",),
        "col_float": ("input_2",),
        "col_int": ("input_2",),
        "col_text": ("input_2",),
        "event_timestamp": ("input_2",),
        "created_at": ("input_2",),
        "cust_id": ("input_2",),
        "double_value": ("assign_1",),
    }


def test_setting_column_as_entity__on_original_frame(snowflake_event_view, mock_get_persistent):
    """
    Test setting a specific column as entity
    """
    _ = mock_get_persistent
    cust_entity = Entity.create(name="customer", serving_name="cust_id")

    # test on original column
    snowflake_event_view.cust_id.as_entity("customer")
    assert id(snowflake_event_view) == id(snowflake_event_view.cust_id.parent)
    assert snowflake_event_view.column_entity_map == {"cust_id": str(cust_entity.id)}

    # test on transformed column
    entity = Entity.create(name="some_random_entity", serving_name="random_id")
    snowflake_event_view["col_int_entity"] = 1 * snowflake_event_view["col_int"]
    snowflake_event_view.col_int_entity.as_entity("some_random_entity")
    assert snowflake_event_view.column_entity_map == {
        "cust_id": str(cust_entity.id),
        "col_int_entity": str(entity.id),
    }

    # test entity column is protected
    with pytest.raises(ValueError) as exc:
        snowflake_event_view["col_int_entity"] = snowflake_event_view["col_int"]
    expected_msg = "Timestamp or entity column 'col_int_entity' cannot be modified!"
    assert expected_msg in str(exc.value)

    # test remove entity
    snowflake_event_view.col_int_entity.as_entity(None)
    assert snowflake_event_view.column_entity_map == {"cust_id": str(cust_entity.id)}


def test_setting_column_as_entity__on_sub_frame(snowflake_event_view, mock_get_persistent):
    """
    Test setting a specific column as entity (on sub-frame)
    """
    _ = mock_get_persistent
    cust_entity = Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    sub_view_first = snowflake_event_view[["cust_id", "col_int"]]
    assert isinstance(sub_view_first, EventView)
    assert sub_view_first.column_entity_map == {"cust_id": str(cust_entity.id)}
    assert set(sub_view_first.columns) == {"event_timestamp", "cust_id", "col_int"}

    # test entity column is protected in sub-frame also
    with pytest.raises(ValueError) as exc:
        sub_view_first["cust_id"] = 10
    expected_msg = "Timestamp or entity column 'cust_id' cannot be modified!"
    assert expected_msg in str(exc.value)

    entity = Entity.create(name="some_random_entity", serving_name="random_id")
    sub_view_second = snowflake_event_view[["col_int", "col_float"]]
    assert sub_view_second.column_entity_map == {}
    sub_view_second.col_int.as_entity("some_random_entity")
    assert sub_view_second.column_entity_map == {"col_int": str(entity.id)}


def test_setting_column_as_entity__invalid_cases(snowflake_event_view):
    """
    Test invalid ways of setting column as entity
    """
    with pytest.raises(RecordRetrievalException) as exc:
        snowflake_event_view.cust_id.as_entity("some_random_entity")
    assert 'Entity name "some_random_entity" not found!' in str(exc.value)

    cust_id = snowflake_event_view.cust_id
    new_cust_id = cust_id * 2
    assert new_cust_id.parent is None
    assert new_cust_id.name is None
    with pytest.raises(ValueError) as exc:
        new_cust_id.as_entity("new_cust_id")
    assert "Series object does not have name!" in str(exc.value)

    new_cust_id.name = "some_random_name"
    with pytest.raises(ValueError) as exc:
        new_cust_id.as_entity("new_cust_id")
    assert "Series object does not have parent frame object!" in str(exc.value)

    with pytest.raises(TypeError) as exc:
        cust_id.as_entity(1234)
    assert 'Unsupported type "<class \'int\'>" for tag name "1234"!' in str(exc.value)


def test_add_description(snowflake_event_view):
    """
    Test add_description to event view's column
    """
    assert snowflake_event_view.column_description_map == {}
    snowflake_event_view.cust_id.add_description("Customer ID column")
    assert snowflake_event_view.column_description_map == {"cust_id": "Customer ID column"}

    with pytest.raises(TypeError) as exc:
        snowflake_event_view.cust_id.add_description(1234)
    assert 'Unsupported type "<class \'int\'>" for description "1234"!' in str(exc.value)
