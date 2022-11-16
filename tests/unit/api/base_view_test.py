"""
Base View test suite
"""
from abc import abstractmethod

import pytest

from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, StrEnum
from featurebyte.query_graph.enum import NodeOutputType, NodeType


class ViewType(StrEnum):
    """
    View API object types
    """

    ITEM_VIEW = "item_view"
    EVENT_VIEW = "event_view"
    DIMENSION_VIEW = "dimension_view"


class BaseViewTestSuite:
    """
    BaseViewTestSuite contains common view tests
    """

    protected_columns = []
    view_type: ViewType = ""
    col = ""
    factory_method = None
    use_data_under_test_in_lineage = False
    view_class = None
    bool_col = col

    @pytest.fixture(name="view_under_test")
    def get_view_under_test_fixture(
        self, snowflake_event_view, snowflake_item_view, snowflake_dimension_view
    ):
        if self.view_type == ViewType.ITEM_VIEW:
            return snowflake_item_view
        if self.view_type == ViewType.EVENT_VIEW:
            return snowflake_event_view
        if self.view_type == ViewType.DIMENSION_VIEW:
            return snowflake_dimension_view
        pytest.fail(
            f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
        )

    @pytest.fixture(name="data_under_test")
    def get_data_under_test_fixture(
        self, snowflake_item_data, snowflake_event_data, snowflake_dimension_data
    ):
        if self.view_type == ViewType.ITEM_VIEW:
            return snowflake_item_data
        if self.view_type == ViewType.EVENT_VIEW:
            return snowflake_event_data
        if self.view_type == ViewType.DIMENSION_VIEW:
            return snowflake_dimension_data
        pytest.fail(
            f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
        )

    def test_setitem__str_key_series_value(self, view_under_test):
        """
        Test assigning Series object to event_view
        """
        source_node_name = view_under_test.node.name
        double_value = view_under_test[self.col] * 2
        assert isinstance(double_value, Series)
        view_under_test["double_value"] = double_value
        assert view_under_test.node.dict(exclude={"name": True}) == {
            "type": NodeType.ASSIGN,
            "parameters": {"name": "double_value", "value": None},
            "output_type": NodeOutputType.FRAME,
        }
        assert view_under_test.column_lineage_map == {
            "col_binary": (source_node_name,),
            "col_boolean": (source_node_name,),
            "col_char": (source_node_name,),
            "col_float": (source_node_name,),
            "col_int": (source_node_name,),
            "col_text": (source_node_name,),
            "event_timestamp": (source_node_name,),
            "created_at": (source_node_name,),
            "cust_id": (source_node_name,),
            "double_value": (view_under_test.node.name,),
        }

    def test_setitem__override_protected_column(self, view_under_test):
        """
        Test attempting to change a view's protected columns
        """
        for column in self.protected_columns:
            assert column in self.protected_columns
            with pytest.raises(ValueError) as exc:
                view_under_test[column] = 1
            expected_msg = f"Column '{column}' cannot be modified!"
            assert expected_msg in str(exc.value)

    def test_getitem__str(self, view_under_test, data_under_test):
        """
        Test retrieving single column
        """
        cust_id = view_under_test[self.col]
        assert isinstance(cust_id, Series)

        assert cust_id.node.dict(exclude={"name": True}) == {
            "type": NodeType.PROJECT,
            "parameters": {"columns": [self.col]},
            "output_type": NodeOutputType.SERIES,
        }
        expected_lineage = (
            (view_under_test.node.name,)
            if not self.use_data_under_test_in_lineage
            else (
                data_under_test.node.name,
                view_under_test.node.name,
            )
        )
        assert cust_id.row_index_lineage == expected_lineage
        assert cust_id.parent.node == view_under_test.node

    @abstractmethod
    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        """
        Assertions for testing that columns updated in _getitem_frame_params are copied over.
        """
        pass

    def test_getitem__series_key(self, view_under_test):
        """
        Test filtering on view object
        """
        mask_cust_id = view_under_test[self.col] < 1000
        assert isinstance(mask_cust_id, Series)
        assert mask_cust_id.dtype == DBVarType.BOOL

        row_subset = view_under_test[mask_cust_id]
        assert isinstance(row_subset, self.view_class)
        assert row_subset.row_index_lineage == (
            view_under_test.row_index_lineage + (row_subset.node.name,)
        )
        self.getitem_frame_params_assertions(row_subset, view_under_test)

    def get_test_view_column_get_item_series_fixture_override(self, view_under_test):
        """
        Override some properties for the view column getitem series test.
        """
        return {}

    def test_view_column_getitem_series(self, view_under_test):
        """
        Test view column filter by boolean mask
        """
        column = view_under_test[self.col]
        overrides = self.get_test_view_column_get_item_series_fixture_override(view_under_test)
        mask = overrides["mask"] if "mask" in overrides else view_under_test[self.bool_col]

        output = column[mask]
        assert output.tabular_data_ids == column.tabular_data_ids
        assert output.name == column.name
        assert output.dtype == column.dtype
        output_dict = output.dict()
        assert output_dict["node_name"] == "filter_1"
        filter_node = next(
            node for node in output_dict["graph"]["nodes"] if node["name"] == "filter_1"
        )
        assert filter_node == {
            "name": "filter_1",
            "type": "filter",
            "parameters": {},
            "output_type": "series",
        }
        expected_edges = [
            {"source": "input_1", "target": "project_1"},
            {"source": "input_1", "target": "project_2"},
            {"source": "project_1", "target": "filter_1"},
            {"source": "project_2", "target": "filter_1"},
        ]
        if "expected_edges" in overrides:
            expected_edges = overrides["expected_edges"]
        assert output_dict["graph"]["edges"] == expected_edges

    def test_unary_op_params(self, view_under_test):
        """
        Test unary operation inherits tabular_data_ids
        """
        column = view_under_test[self.col]
        output = column.isnull()
        assert output.tabular_data_ids == column.tabular_data_ids

    def test_from_data__invalid_input(self):
        """
        Test from_item_data
        """
        with pytest.raises(TypeError) as exc:
            self.factory_method("hello")
        exception_message = str(exc.value)
        assert "type of argument" in exception_message
        assert "got str instead" in exception_message
