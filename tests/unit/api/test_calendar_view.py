"""
Unit test for CalendarView class
"""

import pytest

from featurebyte.api.calendar_view import CalendarView
from featurebyte.exception import JoinViewMismatchError
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.cleaning_operation import (
    DisguisedValueImputation,
    MissingValueImputation,
)
from tests.unit.api.base_view_test import BaseViewTestSuite, ViewType
from tests.util.helper import check_sdk_code_generation


class TestCalendarView(BaseViewTestSuite):
    """
    CalendarView test suite
    """

    protected_columns = ["date", "store_id"]
    view_type = ViewType.CALENDAR_VIEW
    col = "col_int"
    view_class = CalendarView
    bool_col = "col_boolean"
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_int" AS "col_int",
      "col_float" AS "col_float",
      "col_char" AS "col_char",
      CAST("col_text" AS VARCHAR) AS "col_text",
      "col_binary" AS "col_binary",
      "col_boolean" AS "col_boolean",
      CAST("date" AS VARCHAR) AS "date",
      "store_id" AS "store_id",
      (
        "col_int" + 1
      ) AS "new_col"
    FROM "sf_database"."sf_schema"."calendar_table"
    LIMIT 10
    """

    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        assert row_subset.series_id_column == view_under_test.series_id_column


def test_from_calendar_table(snowflake_calendar_table, mock_api_object_cache):
    """
    Test CalendarView creation from CalendarTable
    """
    _ = mock_api_object_cache
    calendar_view = snowflake_calendar_table.get_view()
    expected_view_columns_info = [
        col
        for col in snowflake_calendar_table.columns_info
        if col.name != snowflake_calendar_table.record_creation_timestamp_column
    ]
    assert calendar_view.tabular_source == snowflake_calendar_table.tabular_source
    assert calendar_view.row_index_lineage == snowflake_calendar_table.frame.row_index_lineage
    assert calendar_view.columns_info == expected_view_columns_info
    assert calendar_view.series_id_column == snowflake_calendar_table.series_id_column
    assert (
        calendar_view.calendar_datetime_column == snowflake_calendar_table.calendar_datetime_column
    )
    assert calendar_view.timestamp_column == snowflake_calendar_table.timestamp_column


def test_getitem__list_of_str_contains_protected_column(
    snowflake_calendar_table, snowflake_calendar_view
):
    """
    Test retrieving subset of the calendar view columns
    """
    # select a non-protected column; protected columns (date, store_id) should be auto-included
    calendar_view_subset = snowflake_calendar_view[["col_float"]]
    assert isinstance(calendar_view_subset, CalendarView)
    assert set(calendar_view_subset.column_var_type_map) == {
        "date",
        "store_id",
        "col_float",
    }
    assert calendar_view_subset.row_index_lineage == snowflake_calendar_view.row_index_lineage

    # select a non-protected column with the protected columns explicitly
    calendar_view_subset2 = snowflake_calendar_view[["col_float", "date", "store_id"]]
    assert isinstance(calendar_view_subset2, CalendarView)
    assert set(calendar_view_subset2.column_var_type_map) == {
        "date",
        "store_id",
        "col_float",
    }

    # both subsets should point to the same node
    assert calendar_view_subset.node == calendar_view_subset2.node


def test_calendar_view_copy(snowflake_calendar_view):
    """
    Test CalendarView copy
    """
    new_view = snowflake_calendar_view.copy()
    assert new_view == snowflake_calendar_view
    assert new_view.feature_store == snowflake_calendar_view.feature_store
    assert id(new_view.graph.nodes) == id(snowflake_calendar_view.graph.nodes)

    deep_view = snowflake_calendar_view.copy()
    assert deep_view == snowflake_calendar_view
    assert deep_view.feature_store == snowflake_calendar_view.feature_store
    assert id(deep_view.graph.nodes) == id(snowflake_calendar_view.graph.nodes)

    view_column = snowflake_calendar_view["col_int"]
    new_view_column = view_column.copy()
    assert new_view_column == view_column
    assert new_view_column.parent == view_column.parent == snowflake_calendar_view
    assert id(new_view_column.graph.nodes) == id(view_column.graph.nodes)


def test_validate_join(
    snowflake_event_view,
    snowflake_item_view,
    snowflake_dimension_view,
    snowflake_scd_view,
    snowflake_calendar_view,
):
    """
    Test that CalendarView cannot be used as the left-hand side of a join with any view type.
    """
    for other_view in [
        snowflake_event_view,
        snowflake_item_view,
        snowflake_dimension_view,
        snowflake_scd_view,
        snowflake_calendar_view,
    ]:
        with pytest.raises(
            JoinViewMismatchError,
            match="CalendarView cannot be used as the left-hand side of a join",
        ):
            snowflake_calendar_view.validate_join(other_view)


@pytest.mark.parametrize(
    "left_view_fixture, expected_error",
    [
        ("snowflake_event_view", None),
        ("snowflake_time_series_view", None),
        ("snowflake_snapshots_view", None),
        (
            "snowflake_dimension_view",
            NotImplementedError("Joining a CalendarView to DimensionView is not supported"),
        ),
        (
            "snowflake_scd_view",
            NotImplementedError("Joining a CalendarView to SCDView is not supported"),
        ),
    ],
)
def test_calendar_view_join_as_right(
    request,
    left_view_fixture,
    expected_error,
    snowflake_calendar_view,
):
    """
    Test join combinations with CalendarView as the right-hand side view
    """
    left_view = request.getfixturevalue(left_view_fixture)
    left_subset = left_view[["col_int"]]
    right_subset = snowflake_calendar_view[["col_int"]]

    if expected_error:
        with pytest.raises(
            type(expected_error),
            match=str(expected_error).replace("(", r"\(").replace(")", r"\)"),
        ):
            left_subset.join(right_subset, on="col_int", rsuffix="_cal")
    else:
        joined_view = left_subset.join(right_subset, on="col_int", rsuffix="_cal")
        join_node = joined_view.node
        assert join_node.type == NodeType.JOIN
        join_params = join_node.parameters
        assert join_params.left_on == "col_int"
        assert join_params.right_on == "store_id"
        assert join_params.join_type == "left"
        assert join_params.snapshots_datetime_join_keys is not None


def test_calendar_view_without_series_id(snowflake_database_calendar_table, catalog):
    """
    Test CalendarView from CalendarTable without series_id_column
    """
    _ = catalog
    from featurebyte.query_graph.model.timestamp_schema import TimestampSchema

    calendar_table = snowflake_database_calendar_table.create_calendar_table(
        name="sf_calendar_table_no_series",
        calendar_datetime_column="date",
        calendar_datetime_schema=TimestampSchema(format_string="YYYY-MM-DD"),
    )
    calendar_view = calendar_table.get_view()
    assert isinstance(calendar_view, CalendarView)
    assert calendar_view.series_id_column is None
    # Only date is protected when there's no series_id_column
    assert calendar_view.protected_columns == {"date"}


def test_sdk_code_generation(saved_calendar_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    calendar_view = saved_calendar_table.get_view()
    check_sdk_code_generation(
        calendar_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/calendar_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_calendar_table.id,
    )

    # add some cleaning operations to the table before view construction
    saved_calendar_table.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=-1),
        ]
    )
    saved_calendar_table.col_float.update_critical_data_info(
        cleaning_operations=[
            DisguisedValueImputation(disguised_values=[-99], imputed_value=-1),
        ]
    )

    calendar_view = saved_calendar_table.get_view()
    check_sdk_code_generation(
        calendar_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/calendar_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_calendar_table.id,
    )
