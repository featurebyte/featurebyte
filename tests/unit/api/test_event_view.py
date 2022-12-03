"""
Unit test for EventView class
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.enum import DBVarType
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType


class TestEventView(BaseViewTestSuite):
    """
    EventView test suite
    """

    protected_columns = ["event_timestamp"]
    view_type = ViewType.EVENT_VIEW
    col = "cust_id"
    factory_method = EventView.from_event_data
    view_class = EventView
    bool_col = "col_boolean"

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.default_feature_job_setting == view_under_test.default_feature_job_setting


def test_from_event_data(snowflake_event_data):
    """
    Test from_event_data
    """
    event_view_first = EventView.from_event_data(snowflake_event_data)
    assert event_view_first.tabular_source == snowflake_event_data.tabular_source
    assert event_view_first.node == snowflake_event_data.node
    assert event_view_first.row_index_lineage == snowflake_event_data.row_index_lineage
    assert event_view_first.columns_info == snowflake_event_data.columns_info

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    snowflake_event_data.cust_id.as_entity("customer")
    snowflake_event_data.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        )
    )
    event_view_second = EventView.from_event_data(snowflake_event_data)
    assert event_view_second.columns_info == snowflake_event_data.columns_info
    assert event_view_second.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
    )


def test_getitem__list_of_str(snowflake_event_view):
    """
    Test retrieving subset of the event data features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_view_subset1 = snowflake_event_view[["col_float"]]
    assert isinstance(event_view_subset1, EventView)
    assert set(event_view_subset1.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset1.row_index_lineage == snowflake_event_view.row_index_lineage
    assert (
        event_view_subset1.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # case 2: select a non-protected column with a timestamp column
    event_view_subset2 = snowflake_event_view[["col_float", "event_timestamp"]]
    assert isinstance(event_view_subset2, EventView)
    assert set(event_view_subset2.column_var_type_map) == {"event_timestamp", "col_float"}
    assert event_view_subset2.row_index_lineage == snowflake_event_view.row_index_lineage
    assert (
        event_view_subset2.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # both event data subsets actually point to the same node
    assert event_view_subset1.node == event_view_subset2.node
    assert (
        snowflake_event_view.tabular_data_ids
        == event_view_subset1.tabular_data_ids
        == event_view_subset2.tabular_data_ids
    )


@pytest.mark.parametrize(
    "column, offset, expected_var_type",
    [
        ("event_timestamp", None, DBVarType.TIMESTAMP),
        ("col_float", 1, DBVarType.FLOAT),
        ("col_text", 2, DBVarType.VARCHAR),
    ],
)
def test_event_view_column_lag(snowflake_event_view, column, offset, expected_var_type):
    """
    Test EventViewColumn lag operation
    """
    if offset is None:
        expected_offset_param = 1
    else:
        expected_offset_param = offset
    lag_kwargs = {}
    if offset is not None:
        lag_kwargs["offset"] = offset
    lagged_column = snowflake_event_view[column].lag("cust_id", **lag_kwargs)
    assert lagged_column.node.output_type == NodeOutputType.SERIES
    assert lagged_column.dtype == expected_var_type
    assert lagged_column.node.type == NodeType.LAG
    assert lagged_column.node.parameters == {
        "timestamp_column": "event_timestamp",
        "entity_columns": ["cust_id"],
        "offset": expected_offset_param,
    }
    assert lagged_column.tabular_data_ids == snowflake_event_view[column].tabular_data_ids


def test_event_view_column_lag__invalid(snowflake_event_view):
    """
    Test attempting to apply lag more than once raises error
    """
    snowflake_event_view["prev_col_float"] = snowflake_event_view["col_float"].lag("cust_id")
    with pytest.raises(ValueError) as exc:
        (snowflake_event_view["prev_col_float"] + 123).lag("cust_id")
    assert "lag can only be applied once per column" in str(exc.value)


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


def test_event_view_groupby__prune(snowflake_event_view_with_entity):
    """Test event view groupby pruning algorithm"""
    event_view = snowflake_event_view_with_entity
    feature_job_setting = {
        "blind_spot": "30m",
        "frequency": "1h",
        "time_modulo_frequency": "30m",
    }
    group_by_col = "cust_id"
    a = event_view["a"] = event_view["cust_id"] + 10
    event_view["a_plus_one"] = a + 1
    b = event_view.groupby(group_by_col).aggregate_over(
        "a",
        method="sum",
        windows=["24h"],
        feature_names=["sum_a_24h"],
        feature_job_setting=feature_job_setting,
    )["sum_a_24h"]
    feature = (
        event_view.groupby(group_by_col).aggregate_over(
            "a_plus_one",
            method="sum",
            windows=["24h"],
            feature_names=["sum_a_plus_one_24h"],
            feature_job_setting=feature_job_setting,
        )["sum_a_plus_one_24h"]
        * b
    )
    pruned_graph, mappped_node = feature.extract_pruned_graph_and_node()
    # assign 1 & assign 2 dependency are kept
    assert pruned_graph.edges_map == {
        "input_1": ["project_1", "assign_1"],
        "project_1": ["add_1"],
        "add_1": ["assign_1", "add_2"],
        "add_2": ["assign_2"],
        "assign_1": ["assign_2"],
        "assign_2": ["groupby_1", "groupby_2"],
        "groupby_1": ["project_2"],
        "groupby_2": ["project_3"],
        "project_2": ["mul_1"],
        "project_3": ["mul_1"],
    }


def test_from_event_data_without_event_id_column(snowflake_event_data):
    """
    Test from_event_data when using old EventData without event_id_column (backward compatibility)

    Can probably be removed once DEV-556 is resolved
    """
    snowflake_event_data.__dict__.update({"event_id_column": None})
    event_view = EventView.from_event_data(snowflake_event_data)
    assert event_view.event_id_column is None


def test_validate_join(snowflake_scd_view, snowflake_dimension_view, snowflake_event_view):
    """
    Test validate join
    """
    # No error expected
    snowflake_event_view.validate_join(snowflake_dimension_view)
    snowflake_event_view.validate_join(snowflake_event_view)
