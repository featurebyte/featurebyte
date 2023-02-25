"""
Base View test suite
"""
import time
from abc import abstractmethod

import pytest

from featurebyte import MissingValueImputation
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, StrEnum
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.nested import ColumnCleaningOperation
from tests.util.helper import check_sdk_code_generation


class ViewType(StrEnum):
    """
    View API object types
    """

    ITEM_VIEW = "item_view"
    EVENT_VIEW = "event_view"
    DIMENSION_VIEW = "dimension_view"
    SLOWLY_CHANGING_VIEW = "slowly_changing_view"


class BaseViewTestSuite:
    """
    BaseViewTestSuite contains common view tests
    """

    protected_columns = []
    view_type: ViewType = ""
    col = ""
    factory_method = None
    view_class = None
    bool_col = col

    @pytest.fixture(name="view_under_test")
    def get_view_under_test_fixture(self, request):
        view_type_map = {
            ViewType.DIMENSION_VIEW: "snowflake_dimension_view",
            ViewType.EVENT_VIEW: "snowflake_event_view",
            ViewType.ITEM_VIEW: "snowflake_item_view",
            ViewType.SLOWLY_CHANGING_VIEW: "snowflake_scd_view",
        }
        if self.view_type not in view_type_map:
            pytest.fail(
                f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
            )
        view_name = view_type_map[self.view_type]
        return request.getfixturevalue(view_name)

    @pytest.fixture(name="data_under_test")
    def get_data_under_test_fixture(self, request):
        data_type_map = {
            ViewType.DIMENSION_VIEW: "snowflake_dimension_data",
            ViewType.EVENT_VIEW: "snowflake_event_data",
            ViewType.ITEM_VIEW: "snowflake_item_data",
            ViewType.SLOWLY_CHANGING_VIEW: "snowflake_scd_data",
        }
        if self.view_type not in data_type_map:
            pytest.fail(
                f"Invalid view type `{self.view_type}` found. Please use (or map) a valid ViewType."
            )
        data_name = data_type_map[self.view_type]
        return request.getfixturevalue(data_name)

    @pytest.fixture(name="data_under_test_with_imputation")
    def get_data_under_test_with_imputation_fixture(self, data_under_test):
        factory_kwargs = {}
        if self.view_type == ViewType.ITEM_VIEW:
            factory_kwargs["event_suffix"] = "_event"

        # add some cleaning operations
        data_under_test[self.col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=-1)]
        )
        time.sleep(1)  # wait for 1 second to ensure that the data is updated
        assert len(data_under_test[self.col].info.critical_data_info.cleaning_operations) == 1
        return data_under_test

    def test_auto_view_mode(self, data_under_test_with_imputation):
        """
        Test auto view mode
        """
        factory_kwargs = {}
        if self.view_type == ViewType.ITEM_VIEW:
            factory_kwargs["event_suffix"] = "_event"

        # create view
        view = self.factory_method(data_under_test_with_imputation, **factory_kwargs)

        # check view graph metadata
        metadata = view.node.parameters.metadata
        expected_drop_column_names = []
        if data_under_test_with_imputation.record_creation_date_column:
            expected_drop_column_names.append(
                data_under_test_with_imputation.record_creation_date_column
            )
        assert metadata.view_mode == "auto"
        assert metadata.drop_column_names == expected_drop_column_names
        assert metadata.column_cleaning_operations == []
        assert metadata.data_id == data_under_test_with_imputation.id

        # check that cleaning graph is created
        nested_graph = view.node.parameters.graph
        cleaning_graph_node = nested_graph.get_node_by_name("graph_1")
        assert cleaning_graph_node.parameters.type == "cleaning"

        expected_node_names = ["proxy_input_1", "graph_1", "project_1"]
        if self.view_type == ViewType.ITEM_VIEW:
            expected_node_names = [
                "proxy_input_1",
                "proxy_input_2",
                "graph_1",
                "project_1",
                "join_1",
            ]
        assert list(nested_graph.nodes_map.keys()) == expected_node_names

    def test_manual_view_mode(self, data_under_test_with_imputation):
        """
        Test manual view mode
        """
        factory_kwargs = {}
        if self.view_type == ViewType.ITEM_VIEW:
            factory_kwargs["event_suffix"] = "_event"

        # create view
        view = self.factory_method(
            data_under_test_with_imputation, **factory_kwargs, view_mode="manual"
        )

        # check view graph metadata
        metadata = view.node.parameters.metadata
        assert metadata.view_mode == "manual"
        assert metadata.drop_column_names == []
        assert metadata.column_cleaning_operations == []
        assert metadata.data_id == data_under_test_with_imputation.id

        # check that there is no cleaning graph
        nested_graph = view.node.parameters.graph
        expected_node_names = ["proxy_input_1", "project_1"]
        if self.view_type == ViewType.ITEM_VIEW:
            expected_node_names = ["proxy_input_1", "proxy_input_2", "project_1", "join_1"]
        assert list(nested_graph.nodes_map.keys()) == expected_node_names

    def test_view_mode__auto_manual_equality_check(self, data_under_test_with_imputation):
        """
        Test view mode (create a view in auto mode, then create another equivalent view in manual mode).
        The equality is checked by comparing the view graphs. By using this relationship, we can
        reconstruct the view graph in manual mode from the view graph in auto mode.
        """
        factory_kwargs = {}
        manual_kwargs = {}
        if self.view_type == ViewType.ITEM_VIEW:
            factory_kwargs["event_suffix"] = "_event"
            manual_kwargs["event_join_column_names"] = ["event_timestamp", "cust_id"]

        # create view using auto mode
        view_auto = self.factory_method(data_under_test_with_imputation, **factory_kwargs)

        # create another equivalent view using manual mode
        data_under_test_with_imputation[self.col].update_critical_data_info(cleaning_operations=[])
        drop_column_names = view_auto.node.parameters.metadata.drop_column_names
        view_manual = self.factory_method(
            data_under_test_with_imputation,
            **factory_kwargs,
            view_mode="manual",
            column_cleaning_operations=[
                ColumnCleaningOperation(
                    column_name=self.col,
                    cleaning_operations=[MissingValueImputation(imputed_value=-1)],
                )
            ],
            drop_column_names=drop_column_names,
            **manual_kwargs,
        )

        # check both view graph node inner graph are equal
        assert view_manual.node.parameters.graph == view_auto.node.parameters.graph
        assert (
            view_manual.node.parameters.output_node_name
            == view_auto.node.parameters.output_node_name
        )

    def test_setitem__str_key_series_value(self, view_under_test, data_under_test):
        """
        Test assigning Series object to a view
        """
        double_value = view_under_test[self.col] * 2
        assert isinstance(double_value, Series)
        view_under_test["double_value"] = double_value
        assert view_under_test.node.dict(exclude={"name": True}) == {
            "type": NodeType.ASSIGN,
            "parameters": {"name": "double_value", "value": None},
            "output_type": NodeOutputType.FRAME,
        }

        # check SDK code generation
        check_sdk_code_generation(
            view_under_test,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

    def test_setitem__scalar_value(self, view_under_test, data_under_test):
        """
        Test assigning scalar value to a view
        """
        view_under_test["magic_number"] = 1000
        assert view_under_test.node.dict(exclude={"name": True}) == {
            "type": NodeType.ASSIGN,
            "parameters": {"name": "magic_number", "value": 1000},
            "output_type": NodeOutputType.FRAME,
        }

        # check SDK code generation
        check_sdk_code_generation(
            view_under_test,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

    def test_setitem__override_protected_column(self, view_under_test):
        """
        Test attempting to change a view's protected columns
        """
        for column in view_under_test.protected_columns:
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
        assert cust_id.row_index_lineage == view_under_test.row_index_lineage
        assert cust_id.parent.node == view_under_test.node

        # check SDK code generation
        check_sdk_code_generation(
            cust_id,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

    def test_getitem__list_of_str(self, view_under_test, data_under_test):
        """
        Test getitem with list of columns
        """
        subset_cols = view_under_test[[self.col]]
        assert isinstance(subset_cols, self.view_class)

        # note that protected columns are auto-included
        assert subset_cols.node.dict(exclude={"name": True}) == {
            "type": NodeType.PROJECT,
            "parameters": {"columns": subset_cols.node.parameters.columns},
            "output_type": NodeOutputType.FRAME,
        }
        assert subset_cols.row_index_lineage == view_under_test.row_index_lineage

        # check SDK code generation
        check_sdk_code_generation(
            subset_cols,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

    @abstractmethod
    def getitem_frame_params_assertions(self, row_subset, view_under_test):
        """
        Assertions for testing that columns updated in _getitem_frame_params are copied over.
        """
        pass

    def test_getitem__series_key(self, view_under_test, data_under_test):
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

        # check SDK code generation
        check_sdk_code_generation(
            row_subset,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

    def get_test_view_column_get_item_series_fixture_override(self, view_under_test):
        """
        Override some properties for the view column getitem series test.
        """
        return {}

    def test_view_column_getitem_series(self, view_under_test, data_under_test):
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
            {"source": "input_1", "target": "graph_1"},
            {"source": "graph_1", "target": "project_1"},
            {"source": "graph_1", "target": "project_2"},
            {"source": "project_1", "target": "filter_1"},
            {"source": "project_2", "target": "filter_1"},
        ]
        if "expected_edges" in overrides:
            expected_edges = overrides["expected_edges"]
        assert output_dict["graph"]["edges"] == expected_edges

        # check SDK code generation
        check_sdk_code_generation(
            output,
            to_use_saved_data=False,
            data_id_to_info={
                data_under_test.id: {
                    "name": data_under_test.name,
                    "record_creation_date_column": data_under_test.record_creation_date_column,
                }
            },
        )

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

    def test_join_column_is_part_of_inherited_columns(self, view_under_test):
        """
        Test join column (the primary key / natural key of the view) is part of inherited_columns
        """
        assert view_under_test.get_join_column() in view_under_test.inherited_columns
