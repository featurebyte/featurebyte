"""
Unit tests for classes in featurebyte/query_graph/model/table.py
"""
import textwrap

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.critical_data_info import (
    ConditionOperationField,
    CriticalDataInfo,
    DisguisedValueImputation,
    MissingValueImputation,
    StringValueImputation,
    UnexpectedValueImputation,
    ValueBeyondEndpointImputation,
)
from featurebyte.query_graph.model.table import DimensionTableData, EventTableData, GenericTableData
from featurebyte.query_graph.node.schema import (
    FeatureStoreDetails,
    SnowflakeDetails,
    SourceType,
    TableDetails,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


@pytest.fixture(name="tabular_source")
def tabular_source_fixture():
    """Tabular source fixture"""
    return TabularSource(
        feature_store_id=ObjectId(),
        table_details=TableDetails(
            database_name="db_name", schema_name="schema_name", table_name="table_name"
        ),
    )


@pytest.fixture(name="feature_store_details")
def feature_store_details_fixture():
    """Feature store details fixture"""
    return FeatureStoreDetails(
        type=SourceType.SNOWFLAKE,
        details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            database="sf_database",
            sf_schema="sf_schema",
        ),
    )


@pytest.fixture(name="generic_table_data")
def generic_table_data_fixture(tabular_source):
    """Generic table data fixture"""
    return GenericTableData(
        columns_info=[
            ColumnInfo(name="col_int", dtype=DBVarType.INT),
            ColumnInfo(name="col_float", dtype=DBVarType.FLOAT),
        ],
        tabular_source=tabular_source,
    )


@pytest.fixture(name="event_table_data")
def event_table_data_fixture(tabular_source):
    """Event table data fixture"""
    return EventTableData(
        columns_info=[
            ColumnInfo(name="event_timestamp", dtype=DBVarType.TIMESTAMP),
            ColumnInfo(
                name="amount",
                dtype=DBVarType.FLOAT,
                critical_data_info=CriticalDataInfo(
                    cleaning_operations=[
                        MissingValueImputation(imputed_value=0),
                        ValueBeyondEndpointImputation(
                            type=ConditionOperationField.LESS_THAN, end_point=0, imputed_value=None
                        ),
                    ]
                ),
            ),
        ],
        tabular_source=tabular_source,
        event_timestamp_column="event_timestamp",
    )


@pytest.fixture(name="dimension_table_data")
def dimension_table_data_fixture(tabular_source):
    """Dimension table data fixture"""
    return DimensionTableData(
        columns_info=[
            ColumnInfo(name="user_id", dtype=DBVarType.INT),
            ColumnInfo(
                name="gender",
                dtype=DBVarType.VARCHAR,
                critical_data_info=CriticalDataInfo(
                    cleaning_operations=[
                        UnexpectedValueImputation(
                            expected_values=["male", "female"], imputed_value=None
                        )
                    ]
                ),
            ),
            ColumnInfo(
                name="age",
                dtype=DBVarType.INT,
                critical_data_info=CriticalDataInfo(
                    cleaning_operations=[
                        DisguisedValueImputation(disguised_values=[-999], imputed_value=None),
                        StringValueImputation(imputed_value=None),
                    ]
                ),
            ),
        ],
        tabular_source=tabular_source,
        dimension_id_column="user_id",
    )


@pytest.fixture(name="generic_input_node")
def generic_input_node_fixture(feature_store_details, generic_table_data):
    """Generic table data input node"""
    input_node = generic_table_data.construct_input_node(
        feature_store_details=feature_store_details
    )
    assert input_node.dict() == {
        "type": "input",
        "name": "temp",
        "parameters": {
            "columns": [
                {"name": "col_int", "dtype": "INT"},
                {"name": "col_float", "dtype": "FLOAT"},
            ],
            "feature_store_details": feature_store_details,
            "id": None,
            "table_details": generic_table_data.tabular_source.table_details,
            "type": "generic",
        },
        "output_type": "frame",
    }
    return input_node


@pytest.fixture(name="event_input_node")
def event_input_node_fixture(feature_store_details, event_table_data):
    """Event table data input node"""
    input_node = event_table_data.construct_input_node(feature_store_details=feature_store_details)
    assert input_node.dict() == {
        "type": "input",
        "name": "temp",
        "parameters": {
            "columns": [
                {"name": "event_timestamp", "dtype": "TIMESTAMP"},
                {"name": "amount", "dtype": "FLOAT"},
            ],
            "feature_store_details": feature_store_details,
            "id": event_table_data.id,
            "table_details": event_table_data.tabular_source.table_details,
            "id_column": None,
            "timestamp_column": "event_timestamp",
            "type": "event_data",
        },
        "output_type": "frame",
    }
    return input_node


@pytest.fixture(name="dimension_input_node")
def dimension_input_node_fixture(feature_store_details, dimension_table_data):
    """Dimension table data input node"""
    input_node = dimension_table_data.construct_input_node(
        feature_store_details=feature_store_details
    )
    assert input_node.dict() == {
        "type": "input",
        "name": "temp",
        "parameters": {
            "columns": [
                {"name": "user_id", "dtype": "INT"},
                {"name": "gender", "dtype": "VARCHAR"},
                {"name": "age", "dtype": "INT"},
            ],
            "feature_store_details": feature_store_details,
            "id": dimension_table_data.id,
            "table_details": dimension_table_data.tabular_source.table_details,
            "id_column": "user_id",
            "type": "dimension_data",
        },
        "output_type": "frame",
    }
    return input_node


def test_construct_cleaning_recipe_node__missing_critical_data_info(
    generic_table_data, generic_input_node
):
    """Test construct_cleaning_recipe_node on a table data without any critical data info"""
    output = generic_table_data.construct_cleaning_recipe_node(input_node=generic_input_node)
    assert output is None


def test_construct_cleaning_recipe_node__with_sql_generation(event_table_data, event_input_node):
    """Test construct_cleaning_recipe_node (with sql generation)"""
    # construct an input node & a graph node
    query_graph = QueryGraph()
    inserted_input_node = query_graph.add_node(node=event_input_node, input_nodes=[])
    graph_node = event_table_data.construct_cleaning_recipe_node(input_node=inserted_input_node)
    output_node = query_graph.add_node(node=graph_node, input_nodes=[inserted_input_node])

    # generate query
    graph_interpreter = GraphInterpreter(query_graph=query_graph, source_type=SourceType.SNOWFLAKE)
    output, _ = graph_interpreter.construct_preview_sql(node_name=output_node.name, num_rows=10)
    assert (
        output
        == textwrap.dedent(
            """
        SELECT
          "event_timestamp" AS "event_timestamp",
          CASE
            WHEN (
              CASE WHEN "amount" IS NULL THEN 0 ELSE "amount" END < 0
            )
            THEN NULL
            ELSE CASE WHEN "amount" IS NULL THEN 0 ELSE "amount" END
          END AS "amount"
        FROM "db_name"."schema_name"."table_name"
        LIMIT 10
        """
        ).strip()
    )


def test_construct_cleaning_recipe_node__dimension_data(dimension_table_data, dimension_input_node):
    """Test construct_cleaning_recipe_node (SQL generation is not ready for IS_IN and IS_STRING node)"""
    graph_node = dimension_table_data.construct_cleaning_recipe_node(
        input_node=dimension_input_node
    )
    assert graph_node.parameters.graph.edges_map == {
        "proxy_input_1": ["project_1", "assign_1", "project_2"],
        "project_1": ["is_in_1", "conditional_1"],
        "project_2": ["is_in_2", "conditional_2"],
        "is_in_1": ["not_1"],
        "not_1": ["conditional_1"],
        "is_in_2": ["conditional_2"],
        "conditional_1": ["assign_1"],
        "conditional_2": ["is_string_1", "conditional_3"],
        "conditional_3": ["assign_2"],
        "is_string_1": ["conditional_3"],
        "assign_1": ["assign_2"],
    }
