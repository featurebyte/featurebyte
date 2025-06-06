"""
Unit test for EventView class
"""

import copy
import re
import textwrap
from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import RequestColumn, to_timedelta
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.api.feature import Feature
from featurebyte.api.observation_table import ObservationTable
from featurebyte.enum import DBVarType
from featurebyte.exception import EventViewMatchingEntityColumnNotFound
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TableDetails, TabularSource
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
)
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import (
    check_observation_table_creation_query,
    check_sdk_code_generation,
    compare_pydantic_obj,
    deploy_features_through_api,
    get_node,
)


class TestEventView(BaseViewTestSuite):
    """
    EventView test suite
    """

    protected_columns = ["event_timestamp"]
    view_type = ViewType.EVENT_VIEW
    col = "cust_id"
    view_class = EventView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
      "cust_id" AS "cust_id",
      (
        "cust_id" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."sf_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.default_feature_job_setting == view_under_test.default_feature_job_setting


def test_from_event_table(snowflake_event_table, mock_api_object_cache):
    """
    Test event table creation
    """
    _ = mock_api_object_cache
    event_view_first = snowflake_event_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_event_table.columns_info
        if col.name != snowflake_event_table.record_creation_timestamp_column
    ]
    assert event_view_first.tabular_source == snowflake_event_table.tabular_source
    assert event_view_first.row_index_lineage == snowflake_event_table.frame.row_index_lineage
    assert event_view_first.columns_info == expected_view_columns_info

    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()
    snowflake_event_table.cust_id.as_entity("customer")
    snowflake_event_table.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m")
    )
    event_view_second = snowflake_event_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_event_table.columns_info
        if col.name != snowflake_event_table.record_creation_timestamp_column
    ]
    assert event_view_second.columns_info == expected_view_columns_info
    assert event_view_second.default_feature_job_setting == FeatureJobSetting(
        blind_spot="1m30s", period="6m", offset="3m"
    )


def test_getitem__list_of_str_contains_protected_column(
    snowflake_event_table, snowflake_event_view
):
    """
    Test retrieving subset of the event table features
    """
    # case 1: select a non-protect column without selecting timestamp column and entity identifier column
    event_view_subset1 = snowflake_event_view[["col_float"]]
    assert isinstance(event_view_subset1, EventView)
    assert set(event_view_subset1.column_var_type_map) == {
        "event_timestamp",
        "col_int",
        "col_float",
    }
    assert event_view_subset1.row_index_lineage == snowflake_event_view.row_index_lineage
    assert (
        event_view_subset1.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # case 2: select a non-protected column with a timestamp column
    event_view_subset2 = snowflake_event_view[["col_float", "col_int", "event_timestamp"]]
    assert isinstance(event_view_subset2, EventView)
    assert set(event_view_subset2.column_var_type_map) == {
        "event_timestamp",
        "col_int",
        "col_float",
    }
    assert event_view_subset2.row_index_lineage == snowflake_event_view.row_index_lineage
    assert (
        event_view_subset2.default_feature_job_setting
        == snowflake_event_view.default_feature_job_setting
    )

    # both event table subsets actually point to the same node
    assert event_view_subset1.node == event_view_subset2.node


@pytest.mark.parametrize(
    "column, offset, expected_var_type",
    [
        ("event_timestamp", None, DBVarType.TIMESTAMP_TZ),
        ("col_float", 1, DBVarType.FLOAT),
        ("col_text", 2, DBVarType.VARCHAR),
    ],
)
def test_event_view_column_lag(
    snowflake_event_view, snowflake_event_table, column, offset, expected_var_type
):
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
    compare_pydantic_obj(
        lagged_column.node.parameters,
        expected={
            "timestamp_column": "event_timestamp",
            "entity_columns": ["cust_id"],
            "offset": expected_offset_param,
        },
    )

    # check SDK code generation
    check_sdk_code_generation(
        lagged_column,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_event_table.id: {
                "name": snowflake_event_table.name,
                "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
            }
        },
    )


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


def test_event_view_groupby__prune(
    snowflake_event_view_with_entity, snowflake_event_table_with_entity
):
    """Test event view groupby pruning algorithm"""
    event_view = snowflake_event_view_with_entity
    feature_job_setting = FeatureJobSetting(blind_spot="30m", period="1h", offset="30m")
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
    pruned_graph, mappped_node = feature.extract_pruned_graph_and_node()  # noqa: F841
    # assign 1 & assign 2 dependency are kept
    assert pruned_graph.edges_map == {
        "add_1": ["add_2", "assign_1"],
        "add_2": ["assign_2"],
        "assign_1": ["assign_2"],
        "assign_2": ["groupby_1", "groupby_2"],
        "graph_1": ["project_1", "assign_1"],
        "groupby_1": ["project_2"],
        "groupby_2": ["project_3"],
        "input_1": ["graph_1"],
        "project_1": ["add_1"],
        "project_2": ["mul_1"],
        "project_3": ["mul_1"],
    }

    # check SDK code generation
    event_table_columns_info = snowflake_event_table_with_entity.model_dump(by_alias=True)[
        "columns_info"
    ]
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_event_table_with_entity.id: {
                "name": snowflake_event_table_with_entity.name,
                "record_creation_timestamp_column": snowflake_event_table_with_entity.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail during GroupBy construction
                "columns_info": event_table_columns_info,
            }
        },
    )


def test_validate_join(snowflake_scd_view, snowflake_dimension_view, snowflake_event_view):
    """
    Test validate join
    """
    # No error expected
    snowflake_event_view.validate_join(snowflake_dimension_view)
    snowflake_event_view.validate_join(snowflake_event_view)


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
def get_empty_event_view_fixture(snowflake_event_table_with_entity):
    """
    Get an empty event view.
    """

    def get_event_view():
        return snowflake_event_table_with_entity.get_view()

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
            node_name="",
            tabular_source=TabularSource(
                feature_store_id=PydanticObjectId(ObjectId()),
                table_details=TableDetails(
                    table_name="random",
                ),
            ),
            feature_store=snowflake_feature_store,
        )

    return get_feature


def test_validate_feature_addition__time_based_feature_no_override(
    production_ready_feature, snowflake_event_view_with_entity
):
    """
    Test _validate_feature_addition with no override col provided - expect error
    """
    event_view = snowflake_event_view_with_entity
    with pytest.raises(ValueError) as exc_info:
        event_view._validate_feature_addition("random_col", production_ready_feature, None)
    assert "We currently only support the addition of non-time based features" in str(exc_info)


def test_validate_feature_addition__request_column_derived_feature(
    non_time_based_feature, snowflake_event_view_with_entity
):
    """
    Test _validate_feature_addition with request column derived feature
    """
    req_col_feat = (
        non_time_based_feature
        + (RequestColumn.point_in_time() - RequestColumn.point_in_time()).dt.day
    )
    req_col_feat.name = "req_col_feat"
    event_view = snowflake_event_view_with_entity
    expected_msg = (
        "We currently only support the addition of features that do not use request columns."
    )
    with pytest.raises(ValueError, match=re.escape(expected_msg)):
        event_view.add_feature("random_col", req_col_feat)


def test_validate_feature_addition__time_based_feature_with_override(
    production_ready_feature, snowflake_event_view_with_entity
):
    """
    Test _validate_feature_addition with override col provided - expect error
    """
    event_view = snowflake_event_view_with_entity
    with pytest.raises(ValueError) as exc_info:
        event_view._validate_feature_addition("random_col", production_ready_feature, "random")
    assert "We currently only support the addition of non-time based features" in str(exc_info)


def test_validate_feature_addition__non_time_based_no_override(
    snowflake_event_view_with_entity, non_time_based_feature
):
    """
    Test _validate_feature_addition non-time based with no override col
    """
    # Should run with no errors
    snowflake_event_view_with_entity._validate_feature_addition(
        "random_col", non_time_based_feature, None
    )


def test_validate_feature_addition__non_time_based_with_override(
    snowflake_event_view_with_entity,
    non_time_based_feature,
):
    """
    Test _validate_feature_addition non-time based with override col
    """
    event_view = snowflake_event_view_with_entity
    event_view._validate_feature_addition("random_col", non_time_based_feature, "col_float")


def assert_entity_identifiers_raises_errors(identifiers, feature):
    """
    Helper method to assert that entity identifiers passed in will raise an error on `_get_feature_entity_col`.
    """
    with mock.patch("featurebyte.query_graph.graph.QueryGraph.get_entity_columns") as mock_idents:
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


def test_get_feature_entity_id(
    non_time_based_feature, float_feature, transaction_entity, cust_id_entity
):
    """
    Test get_feature_entity_col
    """
    # verify we can retrieve the entity ID
    entity_id = EventView._get_feature_entity_id(float_feature)

    # construct a feature wil multiple entity IDs
    assert entity_id == cust_id_entity.id
    comp_feature = non_time_based_feature + float_feature
    comp_feature.name = "comp_feature"
    assert comp_feature.graph.get_entity_ids(node_name=comp_feature.node_name) == sorted([
        cust_id_entity.id,
        transaction_entity.id,
    ])

    # verify that multiple entity IDs raises error
    with pytest.raises(ValueError) as exc_info:
        EventView._get_feature_entity_id(comp_feature)
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
    snowflake_event_table_with_entity, float_feature, non_time_based_feature, cust_id_entity
):
    """
    Test _get_view_entity_column - no entity col provided
    """
    event_view = snowflake_event_table_with_entity.get_view()
    assert event_view._get_view_entity_column(float_feature, None) == "cust_id"
    assert event_view._get_view_entity_column(non_time_based_feature, None) == "col_int"

    # remove cust_id entity
    snowflake_event_table_with_entity.cust_id.as_entity(None)
    event_view = snowflake_event_table_with_entity.get_view()
    with pytest.raises(EventViewMatchingEntityColumnNotFound) as exc_info:
        event_view._get_view_entity_column(float_feature, None)
    assert "Unable to find a matching entity column" in str(exc_info)

    # add back cust_id entity
    snowflake_event_table_with_entity.cust_id.as_entity(cust_id_entity.name)


@pytest.fixture(name="generic_input_node_params")
def get_generic_input_node_params_fixture():
    node_params = {
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


def test_add_feature(
    snowflake_event_table, snowflake_event_view, snowflake_item_table, non_time_based_feature
):
    """
    Test add feature
    """
    original_column_info = copy.deepcopy(snowflake_event_view.columns_info)

    # Add feature
    node_name_before = snowflake_event_view.node.name
    new_view = snowflake_event_view.add_feature("new_col", non_time_based_feature, "cust_id")
    assert (
        snowflake_event_view.node.name == node_name_before
    )  # check that original view is not modified

    # assert updated view params
    assert new_view.columns_info == [
        *original_column_info,
        ColumnInfo(
            name="new_col",
            dtype=DBVarType.FLOAT,
        ),
    ]

    join_feature_node_name = new_view.node.name
    assert join_feature_node_name.startswith("join_feature")
    assert new_view.row_index_lineage == (snowflake_event_table.frame.node_name,)

    # assert graph node (excluding name since that can changed by graph pruning)
    view_dict = new_view.model_dump()
    node_dict = get_node(view_dict["graph"], view_dict["node_name"])
    assert node_dict["output_type"] == "frame"
    assert node_dict["type"] == "join_feature"
    assert node_dict["parameters"] == {
        "feature_entity_column": "event_id_col",
        "name": "new_col",
        "view_entity_column": "cust_id",
        "view_point_in_time_column": None,
    }
    assert view_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "input_2", "target": "graph_2"},
        {"source": "graph_1", "target": "graph_2"},
        {"source": "graph_2", "target": "item_groupby_1"},
        {"source": "item_groupby_1", "target": "project_1"},
        {"source": "graph_1", "target": "join_feature_1"},
        {"source": "project_1", "target": "join_feature_1"},
    ]

    # check SDK code generation
    event_table_columns_info = snowflake_event_table.model_dump(by_alias=True)["columns_info"]
    item_table_columns_info = snowflake_item_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        new_view,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_event_table.id: {
                "name": snowflake_event_table.name,
                "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in GroupBy construction
                "columns_info": event_table_columns_info,
            },
            snowflake_item_table.id: {
                "name": snowflake_item_table.name,
                "record_creation_timestamp_column": snowflake_item_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail in GroupBy construction
                "columns_info": item_table_columns_info,
            },
        },
    )


def test_add_feature__inferred_entity_multiple_times(
    snowflake_event_table,
    snowflake_item_table,
    non_time_based_features,
    transaction_entity,
):
    """
    Test calling add_feature() multiple times without specifying entity columns
    """
    _ = snowflake_item_table

    snowflake_event_table[snowflake_event_table.event_id_column].as_entity(transaction_entity.name)
    event_view = snowflake_event_table.get_view()
    original_column_info = copy.deepcopy(event_view.columns_info)

    event_view = event_view.add_feature("new_col", non_time_based_features[0])
    event_view = event_view.add_feature("new_col_1", non_time_based_features[1])

    assert event_view.columns_info == [
        *original_column_info,
        ColumnInfo(
            name="new_col",
            dtype=DBVarType.FLOAT,
        ),
        ColumnInfo(
            name="new_col_1",
            dtype=DBVarType.FLOAT,
        ),
    ]


def test_add_feature__wrong_type(snowflake_event_view):
    """
    Test add feature with invalid type
    """
    with pytest.raises(TypeError) as exc_info:
        col = snowflake_event_view["col_int"]
        snowflake_event_view.add_feature("new_col", col, "cust_id")
    assert (
        str(exc_info.value)
        == 'type of argument "feature" must be Feature; got EventViewColumn instead'
    )


def test_combine_simple_aggregate_with_its_window_aggregate(
    snowflake_event_table, non_time_based_feature
):
    """
    Test combining simple aggregate with its window aggregates after added to EventView
    """
    event_view = snowflake_event_table.get_view()
    event_view = event_view.add_feature("new_col", non_time_based_feature, "col_int")
    new_col_avg_7d = event_view.groupby("cust_id").aggregate_over(
        value_column="new_col",
        method="avg",
        windows=["7d"],
        feature_names=["new_col_avg_7d"],
    )["new_col_avg_7d"]
    new_col_std_7d = event_view.groupby("cust_id").aggregate_over(
        value_column="new_col",
        method="std",
        windows=["7d"],
        feature_names=["new_col_std_7d"],
    )["new_col_std_7d"]

    feature_temp = non_time_based_feature - new_col_avg_7d
    assert non_time_based_feature.output_category == "feature"
    assert new_col_avg_7d.output_category == "feature"
    assert feature_temp.output_category == "feature"

    feature = feature_temp / new_col_std_7d
    assert feature.output_category == "feature"

    feature.name = "final_feature"
    feature.save()


def test_pruned_feature_only_keeps_minimum_required_cleaning_operations(
    snowflake_event_table_with_entity, feature_group_feature_job_setting
):
    """Test pruned feature only keeps minimum required cleaning operations"""
    subset_cols = ["col_int", "col_float"]
    for col in subset_cols:
        snowflake_event_table_with_entity[col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=-1)]
        )

    # create event view & the metadata
    event_view = snowflake_event_table_with_entity.get_view()
    graph_metadata = event_view.node.parameters.metadata
    compare_pydantic_obj(
        graph_metadata.column_cleaning_operations,
        expected=[
            {
                "column_name": "col_int",
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            },
            {
                "column_name": "col_float",
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            },
        ],
    )

    # create feature
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_job_setting=feature_group_feature_job_setting,
        feature_names=["sum_30m"],
    )
    feat = feature_group["sum_30m"]

    # check pruned graph's nested graph nodes
    pruned_graph, node = feat.extract_pruned_graph_and_node()  # noqa: F841
    nested_view_graph_node = pruned_graph.get_node_by_name("graph_1")
    assert nested_view_graph_node.parameters.type == "event_view"

    # note that cleaning operation is not pruned before saving
    compare_pydantic_obj(
        nested_view_graph_node.parameters.metadata.column_cleaning_operations,
        expected=[
            {
                "column_name": "col_int",
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            },
            {
                "column_name": "col_float",
                "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
            },
        ],
    )


def test__validate_column_is_not_used(empty_event_view_builder):
    """
    Test _validate_column_is_not_used
    """
    event_view = empty_event_view_builder()
    col_name = "col_a"
    # no error if column name is unused
    event_view._validate_column_is_not_used(new_column_name=col_name)

    # verify that validation errors if column is used
    event_view.columns_info = [ColumnInfo(name=col_name, dtype=DBVarType.INT)]
    with pytest.raises(ValueError) as exc_info:
        event_view._validate_column_is_not_used(new_column_name=col_name)
    assert "New column name provided is already a column in the existing view" in str(exc_info)


def test_sdk_code_generation(saved_event_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    event_view = saved_event_table.get_view()
    check_sdk_code_generation(
        event_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/event_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )

    # add some cleaning operations to the table before view construction
    saved_event_table.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=-1),
        ]
    )
    saved_event_table.col_float.update_critical_data_info(
        cleaning_operations=[
            DisguisedValueImputation(disguised_values=[-99], imputed_value=-1),
        ]
    )

    event_view = saved_event_table.get_view()
    check_sdk_code_generation(
        event_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/event_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_from_event_view__no_sample(
    snowflake_event_table, snowflake_execute_query, catalog, cust_id_entity
):
    """
    Test creating ObservationTable from an EventView
    """
    _ = catalog
    view = snowflake_event_table.get_view()

    observation_table = view.create_observation_table(
        "my_observation_table_from_event_view",
        columns=["event_timestamp", "cust_id"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        primary_entities=[cust_id_entity.name],
    )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_event_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]
    assert observation_table.request_input.definition is not None

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-4]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          "POINT_IN_TIME",
          "cust_id"
        FROM (
          SELECT
            CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "POINT_IN_TIME",
            "cust_id" AS "cust_id"
          FROM (
            SELECT
              "col_int" AS "col_int",
              "col_float" AS "col_float",
              "col_char" AS "col_char",
              "col_text" AS "col_text",
              "col_binary" AS "col_binary",
              "col_boolean" AS "col_boolean",
              "event_timestamp" AS "event_timestamp",
              "cust_id" AS "cust_id"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
            "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
              "POINT_IN_TIME" IS NOT NULL AND
              "cust_id" IS NOT NULL
        """,
    )
    _, kwargs = snowflake_execute_query.call_args_list[-2]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
          *
        FROM "OBSERVATION_TABLE"
        """,
    )


@pytest.mark.usefixtures("patched_observation_table_service")
def test_create_observation_table_from_event_view__with_sample(
    snowflake_event_table_with_entity, snowflake_execute_query, cust_id_entity
):
    """
    Test creating ObservationTable from an EventView
    """
    view = snowflake_event_table_with_entity.get_view()

    with patch(
        "featurebyte.models.request_input.BaseRequestInput.get_row_count",
        AsyncMock(return_value=1000),
    ):
        observation_table = view.create_observation_table(
            "my_observation_table_from_event_view",
            sample_rows=100,
            columns=["event_timestamp", "cust_id"],
            columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        )

    assert isinstance(observation_table, ObservationTable)
    assert observation_table.name == "my_observation_table_from_event_view"
    assert observation_table.primary_entity_ids == [cust_id_entity.id]

    # Check that the correct query was executed
    _, kwargs = snowflake_execute_query.call_args_list[-4]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          *
        FROM (
          SELECT
            "POINT_IN_TIME",
            "cust_id"
          FROM (
            SELECT
              CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "POINT_IN_TIME",
              "cust_id" AS "cust_id"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "event_timestamp" AS "event_timestamp",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
          )
          WHERE
              "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
                "POINT_IN_TIME" IS NOT NULL AND
                "cust_id" IS NOT NULL
        ) TABLESAMPLE (14)
        ORDER BY
          RANDOM()
        LIMIT 100
        """,
    )
    _, kwargs = snowflake_execute_query.call_args_list[-2]
    check_observation_table_creation_query(
        kwargs["query"],
        """
        CREATE OR REPLACE TABLE "sf_database"."sf_schema"."OBSERVATION_TABLE" AS
        SELECT
          ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
          *
        FROM "OBSERVATION_TABLE"
        """,
    )


def test_shape(snowflake_event_table, snowflake_query_map):
    """
    Test creating ObservationTable from an EventView
    """
    view = snowflake_event_table.get_view()
    expected_call_view = textwrap.dedent(
        """
        WITH data AS (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "col_char" AS "col_char",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "event_timestamp" AS "event_timestamp",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."sf_table"
        )
        SELECT
          COUNT(*) AS "count"
        FROM data
        """
    ).strip()
    expected_call_view_column = textwrap.dedent(
        """
        WITH data AS (
          SELECT
            "col_int" AS "col_int"
          FROM "sf_database"."sf_schema"."sf_table"
        )
        SELECT
          COUNT(*) AS "count"
        FROM data
        """
    ).strip()

    def side_effect(query, timeout=None):
        _ = timeout
        res = snowflake_query_map.get(query)
        if res is not None:
            return pd.DataFrame(res)
        return pd.DataFrame({"count": [1000]})

    with mock.patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.side_effect = side_effect
        assert view.shape() == (1000, 8)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view
        # test view colum shape
        assert view["col_int"].shape() == (1000, 1)
        # Check that the correct query was executed
        assert mock_execute_query.call_args[0][0] == expected_call_view_column


@pytest.mark.flaky(reruns=3)
def test_benchmark_sdk_api_object_operation_runtime(snowflake_event_table):
    """Benchmark runtime of a query constructed from an EventView"""
    event_view = snowflake_event_table.get_view()
    columns = event_view.columns

    # take a single operation runtime by averaging over all columns
    start = datetime.now()
    for col in columns:
        _ = event_view[col]
    single_op_elapsed_time = (datetime.now() - start) / len(columns)

    # add datetime extracted properties
    start = datetime.now()
    column_name = "event_timestamp"
    datetime_series = event_view[column_name]
    properties = [
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_of_week",
        "hour",
        "minute",
        "second",
    ]
    columns = []
    for prop in properties:
        name = f"dt_{prop}"
        event_view[name] = getattr(datetime_series.dt, prop)
        columns.append(name)

    # check timedelta constructed from date difference
    event_view["event_interval"] = datetime_series - datetime_series.lag("cust_id")
    event_view["event_interval_second"] = event_view["event_interval"].dt.second
    event_view["event_interval_hour"] = event_view["event_interval"].dt.hour
    event_view["event_interval_minute"] = event_view["event_interval"].dt.minute
    event_view["event_interval_microsecond"] = event_view["event_interval"].dt.microsecond

    # add timedelta constructed from to_timedelta
    timedelta = to_timedelta(event_view["event_interval_microsecond"].astype(int), "microsecond")
    event_view["timestamp_added"] = datetime_series + timedelta
    event_view["timestamp_added_from_timediff"] = datetime_series + event_view["event_interval"]
    event_view["timestamp_added_constant"] = datetime_series + pd.Timedelta("1d")
    event_view["timedelta_hour"] = timedelta.dt.hour

    # filter on event_interval
    event_view_filtered = event_view[event_view["event_interval_second"] > 500000]
    _ = event_view_filtered.extract_pruned_graph_and_node()

    elapsed = datetime.now() - start
    elapsed_ratio = elapsed.total_seconds() / single_op_elapsed_time.total_seconds()
    assert elapsed_ratio < 2500


def test_event_view_as_feature(
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
    mock_deployment_flow,
):
    """Test offline store table name for event view lookup features"""
    _ = mock_deployment_flow

    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting,
    )
    event_view = snowflake_event_table_with_entity.get_view()
    feature = event_view.as_features(column_names=["col_float"], feature_names=["col_float"])[
        "col_float"
    ]
    feature.save()
    deploy_features_through_api([feature])

    # check offline store table name (should have feature job setting)
    offline_store_info = feature.cached_model.offline_store_info
    ingest_graphs = offline_store_info.extract_offline_store_ingest_query_graphs()
    assert len(ingest_graphs) == 1
    assert ingest_graphs[0].offline_store_table_name == "cat1_transaction_id_1d"


def test_event_view_with_event_timestamp_schema(snowflake_event_table_with_timestamp_schema):
    """
    Test event view with event timestamp schema
    """
    event_view = snowflake_event_table_with_timestamp_schema.get_view()
    assert event_view.event_timestamp_schema.model_dump() == {
        "format_string": None,
        "is_utc_time": False,
        "timezone": {"column_name": "tz_offset", "type": "offset"},
    }
    assert event_view.inherited_columns == {"col_int", "event_timestamp", "tz_offset"}


def test_event_view_cron_feature_job_setting(event_table_with_cron_feature_job_setting):
    """
    Test event view with cron feature job setting
    """
    event_table = event_table_with_cron_feature_job_setting
    event_view = event_table.get_view()
    assert event_view.default_feature_job_setting == CronFeatureJobSetting(
        crontab="0 0 * * *",
        reference_timezone="Etc/UTC",
        blind_spot="600s",
    )
