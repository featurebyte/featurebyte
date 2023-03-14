"""
Test ContextService
"""
import pytest
from bson import ObjectId

from featurebyte.exception import DocumentNotFoundError, DocumentUpdateError
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.generic import JoinNode
from featurebyte.query_graph.node.input import InputNode
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


@pytest.fixture(name="generic_table_node")
def generic_table_node_fixture(input_event_data_node):
    """Input generic table node"""
    input_event_data_node["parameters"]["id"] = None
    input_event_data_node["parameters"]["type"] = "generic"
    return InputNode(**input_event_data_node).dict()


@pytest.fixture(name="invalid_input_event_data_node")
def invalid_input_event_data_node_fixture(input_event_data_node, item_data):
    """Input generic table node"""
    input_event_data_node["parameters"]["id"] = item_data.id
    return input_event_data_node


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
            "left_input_columns": ["event_timestamp", "cust_id"],
            "left_output_columns": ["event_timestamp", "cust_id"],
            "right_input_columns": ["event_id_col", "item_type", "item_amount"],
            "right_output_columns": ["event_id_col", "item_type", "item_amount"],
            "join_type": "inner",
            "scd_parameters": None,
            "metadata": {
                "type": "join_event_data_attributes",
                "columns": ["item_type", "item_amount"],
                "event_suffix": None,
            },
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


@pytest.fixture(name="generic_view_graph")
def generic_view_graph_fixture(generic_table_node):
    """View graph (generic table) fixture"""
    return {"nodes": [generic_table_node]}


def test_context_creation__success(context, entity, user):
    """Check that context fixture is created as expected"""
    assert context.entity_ids == [entity.id]
    assert context.user_id == user.id


@pytest.mark.asyncio
async def test_context_creation__unknown_entity_ids(context_service):
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


@pytest.mark.asyncio
async def test_context_update__validation_error(context_service, context, view_graph, join_node):
    """Check that context update validation error"""
    assert context.graph is None
    query_graph = QueryGraph(**view_graph)

    # case 1: context view is a series but not a frame
    proj_node = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["event_timestamp"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[JoinNode(**join_node)],
    )
    with pytest.raises(DocumentUpdateError) as exc:
        await context_service.update_document(
            document_id=context.id,
            data=ContextUpdate(graph=query_graph, node_name=proj_node.name),
        )
    expected_error = "Context view must but a table but not a single column."
    assert expected_error in str(exc.value)

    # case 2: context view is a feature but not a view
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": "item_type",
        "parent": None,
        "agg_func": "count",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["item_type_count_30d"],
        "windows": ["30d"],
    }
    groupby_node = query_graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[JoinNode(**join_node)],
    )
    with pytest.raises(DocumentUpdateError) as exc:
        await context_service.update_document(
            document_id=context.id,
            data=ContextUpdate(graph=query_graph, node_name=groupby_node.name),
        )
    expected_error = "Context view must be a view but not a feature."
    assert expected_error in str(exc.value)

    # case 3: context view has a missing entity
    proj_node = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["event_timestamp"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[JoinNode(**join_node)],
    )
    with pytest.raises(DocumentUpdateError) as exc:
        await context_service.update_document(
            document_id=context.id,
            data=ContextUpdate(graph=query_graph, node_name=proj_node.name),
        )
    expected_error = "Entities ['customer'] not found in the context view."
    assert expected_error in str(exc.value), exc


@pytest.mark.asyncio
async def test_context_update__validation_error_unsaved_table(
    context_service, context, generic_view_graph
):
    """Check that context update validation error due to unsaved table"""
    with pytest.raises(DocumentUpdateError) as exc:
        await context_service.update_document(
            document_id=context.id,
            data=ContextUpdate(graph=generic_view_graph, node_name="input_1"),
        )
    expected_error = "Data record has not been stored at the persistent."
    assert expected_error in str(exc.value)


@pytest.mark.asyncio
async def test_context_update__validation_error_column_not_found(
    context_service, context, invalid_input_event_data_node
):
    """Check that context update validation error due to column not found in the persisted data"""
    with pytest.raises(DocumentUpdateError) as exc:
        await context_service.update_document(
            document_id=context.id,
            data=ContextUpdate(
                graph={"nodes": [invalid_input_event_data_node]}, node_name="input_1"
            ),
        )
    expected_error = 'Column "col_int" not found in table "sf_item_data".'
    assert expected_error in str(exc.value)
