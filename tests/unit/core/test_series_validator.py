"""
Test series validator module
"""
from __future__ import annotations

import pytest
from bson import ObjectId

from featurebyte.core.frame import Frame
from featurebyte.core.series_validator import _are_series_both_of_type, _validate_entity_ids
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType


def test_validate_entity_ids():
    """
    Test _validate_entity_ids
    """
    # no IDs should error
    with pytest.raises(ValueError) as exc_info:
        _validate_entity_ids([])
    assert "no, or multiple, entity IDs found" in str(exc_info)

    # >1 ID should error
    object_id_1 = PydanticObjectId(ObjectId())
    object_id_2 = PydanticObjectId(ObjectId())
    with pytest.raises(ValueError) as exc_info:
        _validate_entity_ids([object_id_1, object_id_2])
    assert "no, or multiple, entity IDs found" in str(exc_info)

    # exactly one ID shouldn't error
    _validate_entity_ids([object_id_1])


def test_validate_entity():
    """
    Test _validate_entity
    """
    # TODO
    pass


@pytest.fixture(name="get_dataframe_with_type")
def dataframe_fixture(global_graph, snowflake_feature_store):
    """
    Frame test fixture
    """

    def get_data_frame_with_type(table_data_type: TableDataType):
        columns_info = [
            {"name": "CUST_ID", "dtype": DBVarType.INT},
            {"name": "PRODUCT_ACTION", "dtype": DBVarType.VARCHAR},
            {"name": "VALUE", "dtype": DBVarType.FLOAT},
            {"name": "MASK", "dtype": DBVarType.BOOL},
            {"name": "TIMESTAMP_VALUE", "dtype": DBVarType.TIMESTAMP},
        ]
        node = global_graph.add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "type": table_data_type,
                "columns": columns_info,
                "timestamp": "VALUE",
                "table_details": {
                    "database_name": "db",
                    "schema_name": "public",
                    "table_name": "transaction",
                },
                "feature_store_details": {
                    "type": "snowflake",
                    "details": {
                        "database": "db",
                        "sf_schema": "public",
                        "account": "account",
                        "warehouse": "warehouse",
                    },
                },
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        return Frame(
            feature_store=snowflake_feature_store,
            tabular_source={
                "feature_store_id": snowflake_feature_store.id,
                "table_details": {
                    "database_name": "db",
                    "schema_name": "public",
                    "table_name": "some_table_name",
                },
            },
            columns_info=columns_info,
            node_name=node.name,
        )

    return get_data_frame_with_type


def test_are_series_both_of_type(get_dataframe_with_type):
    """
    Test _are_series_both_of_type
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]
    assert not _are_series_both_of_type(event_series, item_series, TableDataType.ITEM_DATA)
    assert not _are_series_both_of_type(item_series, event_series, TableDataType.ITEM_DATA)
    assert _are_series_both_of_type(item_series, item_series, TableDataType.ITEM_DATA)


def test_is_from_same_data():
    """
    Test _is_from_same_data
    """
    # TODO
    pass


def test_is_series_a_lookup_feature():
    """
    Test _is_series_a_lookup_feature
    """
    # TODO
    pass


def test_both_are_lookup_features():
    """
    Test _both_are_lookup_features
    """
    # TODO
    pass


def test_get_event_and_item_data():
    """
    Test _get_event_and_item_data
    """
    # TODO
    pass


def test_is_one_item_and_one_event():
    """
    Test _is_one_item_and_one_event
    """
    # TODO
    pass


def test_item_data_and_event_data_are_related():
    """
    Test _item_data_and_event_data_are_related
    """
    # TODO
    pass


def test_validate_feature_type():
    """
    Test _validate_feature_type
    """
    # TODO
    pass


def test_validate_series():
    """
    Test validate_series
    """
    # TODO
    pass
