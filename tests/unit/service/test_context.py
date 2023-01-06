"""
Test ContextService
"""
import pytest
from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.schema.context import ContextCreate, ContextUpdate


@pytest.fixture(name="input_event_data_node")
def input_event_data_node_fixture(event_data):
    """Input event_data node of a graph"""
    return {
        "name": "input_1",
        "type": "input",
        "output_type": "frame",
        "parameters": {
            "columns": [
                {"dtype": "INT", "name": "col_int"},
                {"dtype": "FLOAT", "name": "col_float"},
                {"dtype": "CHAR", "name": "col_char"},
                {"dtype": "VARCHAR", "name": "col_text"},
                {"dtype": "BINARY", "name": "col_binary"},
                {"dtype": "BOOL", "name": "col_boolean"},
                {"dtype": "TIMESTAMP", "name": "event_timestamp"},
                {"dtype": "TIMESTAMP", "name": "created_at"},
                {"dtype": "INT", "name": "cust_id"},
            ],
            "feature_store_details": {
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
                "type": "snowflake",
            },
            "id": event_data.id,
            "id_column": "col_int",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "timestamp_column": "event_timestamp",
            "type": "event_data",
        },
    }


@pytest.fixture(name="input_item_data_node")
def input_item_data_node_fixture(item_data, event_data):
    """Input item_data node of a graph"""
    return {
        "name": "input_2",
        "type": "input",
        "output_type": "frame",
        "parameters": {
            "columns": [
                {"dtype": "INT", "name": "event_id_col"},
                {"dtype": "VARCHAR", "name": "item_id_col"},
                {"dtype": "VARCHAR", "name": "item_type"},
                {"dtype": "FLOAT", "name": "item_amount"},
                {"dtype": "TIMESTAMP", "name": "created_at"},
            ],
            "feature_store_details": {
                "details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
                "type": "snowflake",
            },
            "id": item_data.id,
            "id_column": "item_id_col",
            "event_data_id": event_data.id,
            "event_id_column": "event_id_col",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "items_table",
            },
            "type": "item_data",
        },
    }


@pytest.fixture(name="join_node")
def join_node_fixture():
    """Join node of a graph"""
    return {
        "name": "join_1",
        "type": "join",
        "output_type": "frame",
        "parameters": {
            "left_on": "col_int",
            "right_on": "event_id_col",
            "left_input_columns": ["event_timestamp", "cust_id", "col_int"],
            "left_output_columns": ["event_timestamp", "cust_id", "event_id_col"],
            "right_input_columns": ["event_id_col", "item_type", "item_amount"],
            "right_output_columns": ["event_id_col", "item_type", "item_amount"],
            "join_type": "inner",
            "scd_parameters": None,
        },
    }


@pytest.fixture(name="view_graph")
def view_graph_fixture(input_event_data_node, input_item_data_node, join_node):
    """View graph fixture"""
    return {
        "nodes": [input_event_data_node, input_item_data_node, join_node],
        "edges": [
            {"source": input_event_data_node["name"], "target": join_node["name"]},
            {"source": input_item_data_node["name"], "target": join_node["name"]},
        ],
    }


def test_context_creation__success(context, entity, user):
    """Check that context fixture is created as expected"""
    assert context.entity_ids == [entity.id]
    assert context.user_id == user.id


@pytest.mark.asyncio
async def test_context_creation__unknown_entity_ids(context_service, user):
    """Check unknown entity ids throws error"""
    random_id = ObjectId()
    with pytest.raises(DocumentNotFoundError) as exc:
        await context_service.create_document(
            data=ContextCreate(name="a_context", entity_ids=[random_id])
        )
    expected_msg = f'Entity (id: "{random_id}") not found. Please save the Entity object first.'
    assert expected_msg in str(exc)


@pytest.mark.asyncio
async def test_context_update__success(context_service, context, view_graph, join_node):
    """Check that context is updated properly"""
    assert context.graph is None
    context = await context_service.update_document(
        document_id=context.id,
        data=ContextUpdate(graph=view_graph, node_name=join_node["name"]),
    )
    assert context.graph == view_graph
    assert context.node_name == join_node["name"]
