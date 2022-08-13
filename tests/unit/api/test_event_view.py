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


def test_from_event_data(snowflake_event_data):
    """
    Test from_event_data
    """
    event_view_first = EventView.from_event_data(snowflake_event_data)
    assert event_view_first.tabular_source == snowflake_event_data.tabular_source
    assert event_view_first.node == snowflake_event_data.node
    assert event_view_first.column_var_type_map == snowflake_event_data.column_var_type_map
    assert event_view_first.row_index_lineage == snowflake_event_data.row_index_lineage
    assert event_view_first.column_entity_map is snowflake_event_data.column_entity_map is None

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    snowflake_event_data.cust_id.as_entity("customer")
    snowflake_event_data.update_default_feature_job_setting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )
    event_view_second = EventView.from_event_data(snowflake_event_data)
    assert event_view_second.column_entity_map == snowflake_event_data.column_entity_map
    assert event_view_second.column_entity_map == {"cust_id": entity.id}
    assert event_view_second.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )


def test_getitem__str(snowflake_event_view):
    """
    Test retrieving single column
    """
    cust_id = snowflake_event_view["cust_id"]
    assert isinstance(cust_id, Series)
    assert cust_id.node.dict(exclude={"name": True}) == {
        "type": NodeType.PROJECT,
        "parameters": {"columns": ["cust_id"]},
        "output_type": NodeOutputType.SERIES,
    }
    assert cust_id.row_index_lineage == (snowflake_event_view.node.name,)
    assert cust_id.parent.node == snowflake_event_view.node


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
    assert (
        snowflake_event_view.event_data_id
        == event_view_subset1.event_data_id
        == event_view_subset2.event_data_id
    )


def test_getitem__series_key(snowflake_event_view):
    """
    Test filtering on event data object
    """
    mask_cust_id = snowflake_event_view["cust_id"] < 1000
    assert isinstance(mask_cust_id, Series)
    assert mask_cust_id.var_type == DBVarType.BOOL

    event_view_row_subset = snowflake_event_view[mask_cust_id]
    assert isinstance(event_view_row_subset, EventView)
    assert event_view_row_subset.row_index_lineage == (
        snowflake_event_view.row_index_lineage + (event_view_row_subset.node.name,)
    )
    assert event_view_row_subset.inception_node == snowflake_event_view.inception_node
    assert (
        event_view_row_subset.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )


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
    source_node_name = snowflake_event_view.node.name
    double_value = snowflake_event_view["col_float"] * 2
    assert isinstance(double_value, Series)
    snowflake_event_view["double_value"] = double_value
    assert snowflake_event_view.node.dict(exclude={"name": True}) == {
        "type": NodeType.ASSIGN,
        "parameters": {"name": "double_value"},
        "output_type": NodeOutputType.FRAME,
    }
    assert snowflake_event_view.column_lineage_map == {
        "col_binary": (source_node_name,),
        "col_boolean": (source_node_name,),
        "col_char": (source_node_name,),
        "col_float": (source_node_name,),
        "col_int": (source_node_name,),
        "col_text": (source_node_name,),
        "event_timestamp": (source_node_name,),
        "created_at": (source_node_name,),
        "cust_id": (source_node_name,),
        "double_value": (snowflake_event_view.node.name,),
    }


def test_setting_column_as_entity__on_original_frame(snowflake_event_view):
    """
    Test setting a specific column as entity
    """
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()

    # test on original column
    snowflake_event_view.cust_id.as_entity("customer")
    assert id(snowflake_event_view) == id(snowflake_event_view.cust_id.parent)
    assert snowflake_event_view.column_entity_map == {"cust_id": cust_entity.id}

    # test on transformed column
    entity = Entity(name="some_random_entity", serving_names=["random_id"])
    entity.save()
    snowflake_event_view["col_int_entity"] = 1 * snowflake_event_view["col_int"]
    snowflake_event_view.col_int_entity.as_entity("some_random_entity")
    assert snowflake_event_view.column_entity_map == {
        "cust_id": cust_entity.id,
        "col_int_entity": entity.id,
    }
    assert snowflake_event_view.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )

    # test entity column is protected
    with pytest.raises(ValueError) as exc:
        snowflake_event_view["col_int_entity"] = snowflake_event_view["col_int"]
    expected_msg = "Timestamp or entity column 'col_int_entity' cannot be modified!"
    assert expected_msg in str(exc.value)

    # test remove entity
    snowflake_event_view.col_int_entity.as_entity(None)
    assert snowflake_event_view.column_entity_map == {"cust_id": cust_entity.id}


def test_setting_column_as_entity__on_sub_frame(snowflake_event_view):
    """
    Test setting a specific column as entity (on sub-frame)
    """
    cust_entity = Entity(name="customer", serving_names=["cust_id"])
    cust_entity.save()
    snowflake_event_view.cust_id.as_entity("customer")
    sub_view_first = snowflake_event_view[["cust_id", "col_int"]]
    assert isinstance(sub_view_first, EventView)
    assert sub_view_first.column_entity_map == {"cust_id": cust_entity.id}
    assert set(sub_view_first.columns) == {"event_timestamp", "cust_id", "col_int"}
    assert (
        sub_view_first.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # test entity column is protected in sub-frame also
    with pytest.raises(ValueError) as exc:
        sub_view_first["cust_id"] = 10
    expected_msg = "Timestamp or entity column 'cust_id' cannot be modified!"
    assert expected_msg in str(exc.value)

    entity = Entity(name="some_random_entity", serving_names=["random_id"])
    entity.save()
    sub_view_second = snowflake_event_view[["col_int", "col_float"]]
    assert sub_view_second.column_entity_map == {}
    sub_view_second.col_int.as_entity("some_random_entity")
    assert sub_view_second.column_entity_map == {"col_int": entity.id}
    assert (
        sub_view_second.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )


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
    assert 'type of argument "entity_name" must be one of (str, NoneType); got int instead' in str(
        exc.value
    )


def test_add_description(snowflake_event_view):
    """
    Test add_description to event view's column
    """
    assert snowflake_event_view.column_description_map == {}
    snowflake_event_view.cust_id.add_description("Customer ID column")
    assert snowflake_event_view.column_description_map == {"cust_id": "Customer ID column"}

    with pytest.raises(TypeError) as exc:
        snowflake_event_view.cust_id.add_description(1234)
    assert 'type of argument "description" must be str; got int instead' in str(exc.value)


def test_unary_op_params(snowflake_event_view):
    """
    Test unary operation inherits event_data_id
    """
    column = snowflake_event_view["cust_id"]
    output = column.isnull()
    assert output.event_data_id == column.event_data_id


def test_event_view_copy(snowflake_event_view):
    """
    Test event view copy
    """
    new_snowflake_event_view = snowflake_event_view.copy()
    assert new_snowflake_event_view == snowflake_event_view
    assert new_snowflake_event_view.feature_store == snowflake_event_view.feature_store
    assert id(new_snowflake_event_view.graph.nodes) == id(snowflake_event_view.graph.nodes)

    deep_snowflake_event_view = snowflake_event_view.copy()
    assert deep_snowflake_event_view == snowflake_event_view
    assert deep_snowflake_event_view.feature_store == snowflake_event_view.feature_store
    assert id(deep_snowflake_event_view.graph.nodes) == id(snowflake_event_view.graph.nodes)

    view_column = snowflake_event_view["col_int"]
    new_view_column = view_column.copy()
    assert new_view_column == view_column
    assert new_view_column.parent == view_column.parent == snowflake_event_view
    assert id(new_view_column.graph.nodes) == id(view_column.graph.nodes)

    deep_view_column = view_column.copy(deep=True)
    assert deep_view_column == view_column
    assert deep_view_column.parent == view_column.parent
    assert id(deep_view_column.graph.nodes) == id(view_column.graph.nodes)
