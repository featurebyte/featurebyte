"""
Unit tests for describe query
"""

from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(autouse=True)
def patch_num_tables_per_join():
    """
    Patch NUM_TABLES_PER_JOIN to 2 for all tests
    """
    with patch("featurebyte.query_graph.sql.interpreter.preview.NUM_TABLES_PER_JOIN", 2):
        yield


@pytest.fixture(name="source_type")
def source_type_fixture():
    """
    Default value for the source_type fixture
    """
    return SourceType.SNOWFLAKE


@pytest.fixture(name="sample_on_primary_table")
def sample_on_primary_table_fixture():
    """
    Default value for the sample_on_primary_table fixture
    """
    return False


def get_graph_with_updated_primary_table_details(query_graph, node_name, to_prune):
    """
    Return graph with updated primary table details to simulate the case where the primary table
    is sampled and cached
    """
    if to_prune:
        query_graph, node_name_map = query_graph.quick_prune(target_node_names=[node_name])
        node_name = node_name_map[node_name]

    # identify primary table node & update the table details
    primary_table_node = query_graph.get_sample_table_node(node_name=node_name)
    assert primary_table_node.parameters.type == "event_table"
    for node in query_graph.nodes:
        if node.name == primary_table_node.name:
            node.parameters.table_details = TableDetails(table_name="cached_sampled_primary_table")
    return query_graph, node_name


@pytest.fixture(name="operation_structure_and_sample_sql")
def operation_structure_and_sample_sql_fixture(
    simple_graph, source_type, source_info, sample_on_primary_table
):
    """
    Fixture for operation structure and sample sql
    """
    graph, node = simple_graph
    source_info = SourceInfo(
        source_type=source_type, database_name="my_db", schema_name="my_schema"
    )
    graph_interpreter = GraphInterpreter(graph, source_info)
    operation_structure = graph_interpreter.extract_operation_structure_for_node(node.name)
    sample_sql_tree, _ = graph_interpreter._construct_sample_sql(
        node_name=node.name,
        num_rows=10,
        seed=1234,
        sample_on_primary_table=sample_on_primary_table,
    )
    return graph, operation_structure, sample_sql_tree


@pytest.mark.parametrize("sample_on_primary_table", [False, True])
@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
def test_graph_interpreter_describe(
    operation_structure_and_sample_sql, source_type, update_fixtures
):
    """Test graph sample"""
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(
        graph, SourceInfo(source_type=source_type, database_name="my_db", schema_name="my_schema")
    )

    sql_code = sql_to_string(
        interpreter.construct_describe_queries([operation_structure.columns], sample_sql_tree)
        .queries[0]
        .expr,
        source_type,
    )
    expected_filename = f"tests/fixtures/query_graph/expected_describe_{source_type.lower()}.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_describe_specify_stats_names(
    operation_structure_and_sample_sql, update_fixtures, source_info
):
    """Test describe sql with only required stats names"""
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    describe_query = interpreter.construct_describe_queries(
        [operation_structure.columns[:]], sample_sql_tree, stats_names=["min", "max"]
    ).queries[0]
    assert describe_query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_stats_names.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(describe_query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_describe_specify_count_based_stats_only(
    operation_structure_and_sample_sql, update_fixtures, source_info
):
    """
    Test describe sql with only count based stats
    """
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    describe_query = interpreter.construct_describe_queries(
        [operation_structure.columns[:]], sample_sql_tree, stats_names=["entropy"]
    ).queries[0]
    assert describe_query.row_names == ["dtype", "entropy"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_count_based_stats_only.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(describe_query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_describe_specify_empty_stats(
    operation_structure_and_sample_sql, update_fixtures, source_info
):
    """
    Test describe sql with empty stats edge case
    """
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    describe_query = interpreter.construct_describe_queries(
        [operation_structure.columns[:]], sample_sql_tree, stats_names=[]
    ).queries[0]
    assert describe_query.row_names == ["dtype"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_empty_stats.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(describe_query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_describe_in_batches(operation_structure_and_sample_sql, update_fixtures, source_info):
    """Test describe sql in batches"""
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    column_groups = [
        operation_structure.columns[i : i + 3]
        for i in range(0, len(operation_structure.columns), 3)
    ]
    describe_queries = interpreter.construct_describe_queries(
        column_groups,
        sample_sql_tree,
        stats_names=["min", "max"],
    )
    assert len(describe_queries.queries) == 2

    query = describe_queries.queries[0]
    assert query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in query.columns] == [
        "ts",
        "cust_id",
        "a",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_batches_0.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )

    query = describe_queries.queries[1]
    assert query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in query.columns] == [
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_batches_1.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_describe_no_batches(operation_structure_and_sample_sql, update_fixtures, source_info):
    """Test describe sql and disable batching by setting batch size to 0"""
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    describe_queries = interpreter.construct_describe_queries(
        [operation_structure.columns[:]],
        sample_sql_tree,
        stats_names=["dtype", "entropy"],
    )
    assert len(describe_queries.queries) == 1
    describe_query = describe_queries.queries[0]
    assert describe_query.row_names == ["dtype", "entropy"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_count_based_stats_only.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(describe_query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_describe_with_date_range_and_size(
    operation_structure_and_sample_sql, update_fixtures, source_info
):
    """Test describe sql with only required stats names"""
    graph, operation_structure, sample_sql_tree = operation_structure_and_sample_sql
    interpreter = GraphInterpreter(graph, source_info)

    describe_query = interpreter.construct_describe_queries(
        [operation_structure.columns[:]],
        sample_sql_tree,
        stats_names=["min", "max"],
    ).queries[0]
    assert describe_query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_date_range_and_size.sql"
    assert_equal_with_expected_fixture(
        sql_to_string(describe_query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
    )


def test_value_counts_sql_no_casting(graph, node_input, update_fixtures, source_info):
    """Test value counts sql"""
    interpreter = GraphInterpreter(graph, source_info)
    value_counts_queries = interpreter.construct_value_counts_sql(
        node_input.name,
        column_names=["a", "b"],
        num_rows=50000,
        num_categories_limit=1000,
        total_num_rows=100000,
    )
    assert len(value_counts_queries.queries) == 2
    for query in value_counts_queries.queries:
        expected_filename = (
            f"tests/fixtures/query_graph/expected_value_counts_{query.column_name}.sql"
        )
        assert_equal_with_expected_fixture(
            sql_to_string(query.expr, SourceType.SNOWFLAKE), expected_filename, update_fixtures
        )


def test_graph_interpreter_describe_event_join_scd_view(update_fixtures, source_info):
    """Test graph sample"""
    table_details = {"database_name": "FEATUREBYTE_TESTING", "schema_name": "GROCERY"}
    event_table_id, scd_table_id = ObjectId(), ObjectId()

    query_graph = QueryGraphModel()
    input_event_node = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": [
                {"dtype": "VARCHAR", "name": "GroceryInvoiceGuid"},
                {"dtype": "VARCHAR", "name": "GroceryCustomerGuid"},
                {"dtype": "TIMESTAMP", "name": "Timestamp"},
                {"dtype": "TIMESTAMP", "name": "record_available_at"},
                {"dtype": "FLOAT", "name": "Amount"},
            ],
            "event_timestamp_timezone_offset": None,
            "event_timestamp_timezone_offset_column": None,
            "feature_store_details": {"details": None, "type": "snowflake"},
            "id": event_table_id,
            "id_column": "GroceryInvoiceGuid",
            "table_details": {**table_details, "table_name": "GROCERYINVOICE"},
            "timestamp_column": "Timestamp",
            "type": "event_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    input_scd_node = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": [
                {"dtype": "VARCHAR", "name": "RowID"},
                {"dtype": "VARCHAR", "name": "GroceryCustomerGuid"},
                {"dtype": "TIMESTAMP", "name": "ValidFrom"},
                {"dtype": "VARCHAR", "name": "Gender"},
                {"dtype": "TIMESTAMP", "name": "record_available_at"},
                {"dtype": "BOOL", "name": "CurrentRecord"},
            ],
            "current_flag_column": "CurrentRecord",
            "effective_timestamp_column": "ValidFrom",
            "end_timestamp_column": None,
            "feature_store_details": {"details": None, "type": "snowflake"},
            "id": scd_table_id,
            "natural_key_column": "GroceryCustomerGuid",
            "surrogate_key_column": "RowID",
            "table_details": {**table_details, "table_name": "GROCERYUSER"},
            "type": "scd_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    event_graph_node = query_graph.add_operation(
        node_type=NodeType.GRAPH,
        node_params={
            "graph": {
                "edges": [{"source": "proxy_input_1", "target": "project_1"}],
                "nodes": [
                    {
                        "name": "proxy_input_1",
                        "output_type": "frame",
                        "parameters": {"input_order": 0},
                        "type": "proxy_input",
                    },
                    {
                        "name": "project_1",
                        "output_type": "frame",
                        "parameters": {
                            "columns": [
                                "GroceryInvoiceGuid",
                                "GroceryCustomerGuid",
                                "Timestamp",
                                "Amount",
                            ]
                        },
                        "type": "project",
                    },
                ],
            },
            "metadata": {
                "column_cleaning_operations": [],
                "drop_column_names": ["record_available_at"],
                "table_id": event_table_id,
                "view_mode": "auto",
            },
            "output_node_name": "project_1",
            "type": "event_view",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_event_node],
    )
    scd_graph_node = query_graph.add_operation(
        node_type=NodeType.GRAPH,
        node_params={
            "graph": {
                "edges": [{"source": "proxy_input_1", "target": "project_1"}],
                "nodes": [
                    {
                        "name": "proxy_input_1",
                        "output_type": "frame",
                        "parameters": {"input_order": 0},
                        "type": "proxy_input",
                    },
                    {
                        "name": "project_1",
                        "output_type": "frame",
                        "parameters": {
                            "columns": [
                                "RowID",
                                "GroceryCustomerGuid",
                                "ValidFrom",
                                "Gender",
                            ]
                        },
                        "type": "project",
                    },
                ],
            },
            "metadata": {
                "column_cleaning_operations": [],
                "drop_column_names": ["record_available_at", "CurrentRecord"],
                "table_id": scd_table_id,
                "view_mode": "auto",
            },
            "output_node_name": "project_1",
            "type": "scd_view",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_scd_node],
    )
    join_node = query_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params={
            "join_type": "left",
            "left_input_columns": [
                "GroceryInvoiceGuid",
                "GroceryCustomerGuid",
                "Timestamp",
                "Amount",
            ],
            "left_on": "GroceryCustomerGuid",
            "left_output_columns": [
                "GroceryInvoiceGuid",
                "GroceryCustomerGuid",
                "Timestamp",
                "Amount",
            ],
            "metadata": {"rprefix": "", "rsuffix": "", "type": "join"},
            "right_input_columns": [
                "Gender",
            ],
            "right_on": "GroceryCustomerGuid",
            "right_output_columns": [
                "Gender",
            ],
            "scd_parameters": {
                "current_flag_column": "CurrentRecord",
                "effective_timestamp_column": "ValidFrom",
                "end_timestamp_column": None,
                "left_timestamp_column": "Timestamp",
                "natural_key_column": "GroceryCustomerGuid",
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_graph_node, scd_graph_node],
    )

    # identify primary table node & update the table details
    query_graph, node_name = get_graph_with_updated_primary_table_details(
        query_graph, join_node.name, to_prune=False
    )
    interpreter = GraphInterpreter(query_graph, source_info)
    sample_sql_tree, _ = interpreter._construct_sample_sql(
        node_name=node_name,
        num_rows=10,
        seed=1234,
        total_num_rows=1000,
        sample_on_primary_table=True,
    )

    expected_filename = "tests/fixtures/query_graph/expected_primary_table_sampled_data.sql"
    assert_equal_with_expected_fixture(
        sample_sql_tree.sql(pretty=True), expected_filename, update_fixtures
    )


def test_describe__with_primary_table_sampling_on_graph_containing_inner_join(
    global_graph,
    item_table_join_event_table_node,
    update_fixtures,
    source_info,
):
    """Test describe queries with primary table sampling on graph containing inner join or filter"""
    query_graph, mapped_node_name = get_graph_with_updated_primary_table_details(
        query_graph=global_graph,
        node_name=item_table_join_event_table_node.name,
        to_prune=True,
    )
    interpreter = GraphInterpreter(query_graph, source_info)
    sample_sql_tree, _ = interpreter._construct_sample_sql(
        node_name=mapped_node_name,
        num_rows=10,
        seed=1234,
        total_num_rows=1000,
        sample_on_primary_table=True,
    )
    expected_filename = "tests/fixtures/query_graph/expected_item_table_join_event_table_primary_table_sampled_data.sql"
    assert_equal_with_expected_fixture(
        sample_sql_tree.sql(pretty=True), expected_filename, update_fixtures
    )


def test_describe__with_primary_table_sampling_on_graph_containing_filter(
    global_graph,
    item_table_input_node,
    event_table_input_node,
    join_node_params,
    update_fixtures,
    source_info,
):
    """Test describe queries with primary table sampling on graph containing inner join or filter"""
    node_params = join_node_params.copy()
    node_params["join_type"] = "left"
    node_join = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_table_input_node, item_table_input_node],
    )

    interpreter = GraphInterpreter(global_graph, source_info)

    # sanity check on describe query without filter node & no inner join
    sample_sql_tree, _ = interpreter._construct_sample_sql(
        node_name=node_join.name,
        num_rows=10,
        seed=1234,
        total_num_rows=1000,
        sample_on_primary_table=True,
    )
    assert "LIMIT 10" in sample_sql_tree.sql(pretty=True)  # no over sampling

    # add a filter operation
    node_proj_oder_id = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["order_id"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_join],
    )
    node_eq = global_graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj_oder_id],
    )
    filter_node = global_graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_join, node_eq],
    )

    # identify primary table node & update the table details
    query_graph, mapped_node_name = get_graph_with_updated_primary_table_details(
        query_graph=global_graph, node_name=filter_node.name, to_prune=True
    )

    # check describe query with query graph containing filter node & join operation
    interpreter = GraphInterpreter(query_graph, source_info)
    sample_sql_tree, _ = interpreter._construct_sample_sql(
        node_name=mapped_node_name,
        num_rows=10,
        seed=1234,
        total_num_rows=1000,
        sample_on_primary_table=True,
    )

    expected_filename = (
        "tests/fixtures/query_graph/expected_filtered_table_primary_table_sampled_data.sql"
    )
    assert_equal_with_expected_fixture(
        sample_sql_tree.sql(pretty=True), expected_filename, update_fixtures
    )


def test_construct_sample_sql(simple_graph, update_fixtures, source_info):
    """
    Test setting sort_by_prob=False in construct_sample_sql
    """
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_info)
    sql_code = sql_to_string(
        interpreter._construct_sample_sql(
            node.name,
            num_rows=1000,
            total_num_rows=10000,
            seed=1234,
            sample_on_primary_table=False,
            sort_by_prob=False,
        )[0],
        source_info.source_type,
    )
    expected_filename = "tests/fixtures/query_graph/expected_sample_sql_disable_sort_by_prob.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)
