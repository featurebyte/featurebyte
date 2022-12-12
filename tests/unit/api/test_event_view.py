"""
Unit test for EventView class
"""
from typing import List

from unittest import mock
from unittest.mock import PropertyMock, patch

import pytest
from bson import ObjectId

from featurebyte import Feature
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.enum import DBVarType
from featurebyte.exception import EventViewMatchingEntityColumnNotFound
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_store import ColumnInfo, TableDetails, TabularSource
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


@pytest.fixture(name="production_ready_feature")
def production_ready_feature_fixture(feature_group):
    """Fixture for a production ready feature"""
    feature = feature_group["sum_30m"] + 123
    feature.name = "production_ready_feature"
    assert feature.parent is None
    feature.__dict__["readiness"] = FeatureReadiness.PRODUCTION_READY
    feature.__dict__["version"] = "V220401"
    feature_group["production_ready_feature"] = feature
    return feature


def test_validate_entity_col_override__invalid_columns(snowflake_event_view):
    """
    Test _validate_entity_col_override
    """
    with pytest.raises(ValueError) as exc_info:
        snowflake_event_view._validate_entity_col_override("")
    assert "is an empty string" in str(exc_info)

    with pytest.raises(ValueError) as exc_info:
        snowflake_event_view._validate_entity_col_override("random_col")
    assert "not a column in the event view" in str(exc_info)


def test_validate_entity_col_override__valid_col_passed_in(snowflake_event_view):
    """
    Test _validate_entity_col_override
    """
    col_to_use = snowflake_event_view.columns_info[0].name
    # No error raised
    snowflake_event_view._validate_entity_col_override(col_to_use)


@pytest.fixture(name="empty_event_view_builder")
def get_empty_event_view_fixture(snowflake_feature_store):
    """
    Get an empty event view.
    """

    def get_event_view():
        return EventView(
            columns_info=[],
            row_index_lineage=[],
            node_name="",
            tabular_source=TabularSource(
                feature_store_id=PydanticObjectId(ObjectId()),
                table_details=TableDetails(
                    table_name="random",
                ),
            ),
            tabular_data_ids=[],
            column_lineage_map={},
            feature_store=snowflake_feature_store,
        )

    return get_event_view


@pytest.fixture(name="empty_feature_builder")
def get_empty_feature_fixture(snowflake_feature_store):
    """
    Helper to get an empty feature
    """

    def get_feature():
        return Feature(
            entity_ids=[],
            dtype=DBVarType.INT,
            row_index_lineage=[],
            node_name="",
            tabular_source=TabularSource(
                feature_store_id=PydanticObjectId(ObjectId()),
                table_details=TableDetails(
                    table_name="random",
                ),
            ),
            tabular_data_ids=[],
            feature_store=snowflake_feature_store,
        )

    return get_feature


def test_validate_feature_addition__time_based_feature_no_override(
    production_ready_feature, empty_event_view_builder
):
    """
    Test _validate_feature_addition with no override col provided - expect error
    """
    event_view = empty_event_view_builder()
    with pytest.raises(ValueError) as exc_info:
        event_view._validate_feature_addition(production_ready_feature, None)
    assert "We currently only support the addition of non-time based features" in str(exc_info)


def test_validate_feature_addition__time_based_feature_with_override(
    production_ready_feature, empty_event_view_builder
):
    """
    Test _validate_feature_addition with override col provided - expect error
    """
    event_view = empty_event_view_builder()
    with pytest.raises(ValueError) as exc_info:
        event_view._validate_feature_addition(production_ready_feature, "random")
    assert "We currently only support the addition of non-time based features" in str(exc_info)


def test_validate_feature_addition__non_time_based_no_override(
    empty_feature_builder, empty_event_view_builder
):
    """
    Test _validate_feature_addition non-time based with no override col
    """
    event_view = empty_event_view_builder()
    empty_feature = empty_feature_builder()
    # Should run with no errors
    with mock.patch(
        "featurebyte.api.feature.Feature.is_time_based", new_callable=PropertyMock
    ) as mock_is_time_based:
        mock_is_time_based.return_value = False
        event_view._validate_feature_addition(empty_feature, None)


def test_validate_feature_addition__non_time_based_with_override(
    event_view_with_col_infos, empty_feature_builder
):
    """
    Test _validate_feature_addition non-time based with override col
    """
    col_name = "col_a"
    event_view = event_view_with_col_infos([ColumnInfo(name=col_name, dtype=DBVarType.INT)])
    empty_feature = empty_feature_builder()
    # Should run with no errors
    with mock.patch(
        "featurebyte.api.feature.Feature.is_time_based", new_callable=PropertyMock
    ) as mock_is_time_based:
        mock_is_time_based.return_value = False
        event_view._validate_feature_addition(empty_feature, col_name)


def assert_entity_identifiers_raises_errors(identifiers, feature):
    """
    Helper method to assert that entity identifiers passed in will raise an error on `_get_feature_entity_col`.
    """
    with mock.patch(
        "featurebyte.api.feature.Feature.entity_identifiers", new_callable=PropertyMock
    ) as mock_idents:
        mock_idents.return_value = identifiers
        with pytest.raises(ValueError) as exc_info:
            EventView._get_feature_entity_col(feature)
        assert "The feature should only be based on one entity" in str(exc_info)


def test_get_feature_entity_col(production_ready_feature):
    """
    Test get_feature_entity_col
    """
    # verify we can retrieve the entity
    col = EventView._get_feature_entity_col(production_ready_feature)
    assert col == "cust_id"

    # verify that no entity identifiers raises error
    assert_entity_identifiers_raises_errors([], production_ready_feature)

    # verify that multiple entity identifiers raises error
    assert_entity_identifiers_raises_errors(["col_a", "col_b"], production_ready_feature)


@pytest.fixture(name="feature_with_entity_ids")
def get_feature_with_entity_ids(snowflake_feature_store):
    """
    Get a function that helps us create test features with configurable entity IDs.
    """

    def get_feature(entity_ids):
        return Feature(
            entity_ids=entity_ids,
            dtype=DBVarType.INT,
            row_index_lineage=[],
            node_name="",
            tabular_source=TabularSource(
                feature_store_id=PydanticObjectId(ObjectId()),
                table_details=TableDetails(
                    table_name="random",
                ),
            ),
            tabular_data_ids=[],
            feature_store=snowflake_feature_store,
        )

    return get_feature


def test_get_feature_entity_id(feature_with_entity_ids):
    """
    Test get_feature_entity_col
    """
    entity_id_1 = PydanticObjectId(ObjectId())
    entity_id_2 = PydanticObjectId(ObjectId())
    # verify we can retrieve the entity ID
    feature = feature_with_entity_ids([entity_id_1])
    entity_id = EventView._get_feature_entity_id(feature)
    assert entity_id == feature.entity_ids[0]

    # verify that no entity IDs raises error
    feature = feature_with_entity_ids([])
    with pytest.raises(ValueError) as exc_info:
        EventView._get_feature_entity_id(feature)
    assert "The feature should only be based on one entity" in str(exc_info)

    # verify that multiple entity IDs raises error
    feature = feature_with_entity_ids([entity_id_1, entity_id_2])
    with pytest.raises(ValueError) as exc_info:
        EventView._get_feature_entity_id(feature)
    assert "The feature should only be based on one entity" in str(exc_info)


@pytest.fixture(name="event_view_with_col_infos")
def get_event_view_with_col_infos(empty_event_view_builder):
    """
    Get a function that helps us create test event_views with configurable column infos.
    """

    def get_event_view(col_info):
        event_view = empty_event_view_builder()
        event_view.columns_info = col_info
        return event_view

    return get_event_view


def test_get_col_with_entity_id(event_view_with_col_infos):
    """
    Test _get_col_with_entity_id
    """
    entity_id = PydanticObjectId(ObjectId())
    # No column infos returns None
    event_view = event_view_with_col_infos([])
    col = event_view._get_col_with_entity_id(entity_id)
    assert col is None

    # Exactly one matching column info should return the column name
    col_a_name = "col_a"
    col_info_a = ColumnInfo(name=col_a_name, dtype=DBVarType.INT, entity_id=entity_id)
    entity_id_b = PydanticObjectId(ObjectId())
    diff_entity_id_col_info_b = ColumnInfo(name="col_b", dtype=DBVarType.INT, entity_id=entity_id_b)
    event_view = event_view_with_col_infos([col_info_a, diff_entity_id_col_info_b])
    col = event_view._get_col_with_entity_id(entity_id)
    assert col is col_a_name

    # Multiple matching column info should return None
    same_entity_id_col_info_c = ColumnInfo(name="col_c", dtype=DBVarType.INT, entity_id=entity_id)
    event_view = event_view_with_col_infos([col_info_a, same_entity_id_col_info_c])
    col = event_view._get_col_with_entity_id(entity_id)
    assert col is None


def test_get_view_entity_column__entity_col_provided(
    snowflake_event_view, production_ready_feature
):
    """
    Test _get_view_entity_column - entity col provided
    """
    # Test empty string passed in - should have no error since we assume validation is done prior.
    col_to_use = snowflake_event_view._get_view_entity_column(production_ready_feature, "")
    assert col_to_use == ""

    # Test column is passed in - we don't have to validate whether the column exists in the view, since we
    # would've done that beforehand already.
    entity_col_name = "entity_col"
    col_to_use = snowflake_event_view._get_view_entity_column(
        production_ready_feature, entity_col_name
    )
    assert col_to_use == entity_col_name


def test_get_view_entity_column__no_entity_col_provided(
    event_view_with_col_infos, feature_with_entity_ids
):
    """
    Test _get_view_entity_column - no entity col provided
    """
    entity_id_1 = PydanticObjectId(ObjectId())
    entity_id_2 = PydanticObjectId(ObjectId())

    # No matching column should throw an error
    feature = feature_with_entity_ids([entity_id_1])
    event_view_with_no_col_info = event_view_with_col_infos(
        [
            ColumnInfo(name="random", dtype=DBVarType.INT, entity_id=entity_id_2),
        ]
    )
    with pytest.raises(EventViewMatchingEntityColumnNotFound) as exc_info:
        event_view_with_no_col_info._get_view_entity_column(feature, None)
    assert "Unable to find a matching entity column" in str(exc_info)

    # Matching column should match
    col_a_name = "col_a"
    event_view_with_matching_col_info = event_view_with_col_infos(
        [
            ColumnInfo(name="random", dtype=DBVarType.INT, entity_id=entity_id_2),
            ColumnInfo(name=col_a_name, dtype=DBVarType.INT, entity_id=entity_id_1),
        ]
    )
    view_entity_col = event_view_with_matching_col_info._get_view_entity_column(feature, None)
    assert view_entity_col == col_a_name
