"""
Test series validator module
"""
from __future__ import annotations

from typing import Optional

import pytest
from bson import ObjectId

from featurebyte import Entity
from featurebyte.core.frame import Frame
from featurebyte.core.series_validator import (
    _are_series_both_of_type,
    _both_are_lookup_features,
    _get_event_and_item_data_series,
    _get_event_data_id_of_item_series,
    _is_from_same_data,
    _is_one_item_and_one_event,
    _is_parent_child,
    _item_data_and_event_data_are_related,
    _series_data_type,
    _series_tabular_data_id,
    _validate_entity_ids,
    validate_entities,
    validate_feature_type,
)
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import ParentEntity
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


def test_is_parent_child():
    """
    Test _is_parent_child
    """
    object_id_a = PydanticObjectId(ObjectId())
    object_id_b = PydanticObjectId(ObjectId())
    entity_a = Entity(id=object_id_a, name="customer", serving_names=["cust_id"])
    entity_b = Entity(id=object_id_b, name="user", serving_names=["user_id"])
    assert not _is_parent_child(entity_a, entity_b)

    parents = [
        ParentEntity(
            id=entity_a.id,
            data_type=TableDataType.EVENT_DATA,
            data_id=PydanticObjectId(ObjectId()),
        )
    ]
    object_id_c = PydanticObjectId(ObjectId())
    entity_c = Entity(
        id=object_id_c, name="customer child", serving_names=["child_id"], parents=parents
    )
    assert _is_parent_child(entity_a, entity_c)
    assert not _is_parent_child(entity_c, entity_a)


@pytest.mark.asyncio
async def test_validate_entities(app_container):
    """
    Test validate entities
    """
    entity_a = Entity(name="customer", serving_names=["cust_id"])
    entity_b = Entity(name="user", serving_names=["user_id"])
    entity_c = Entity(name="customer child", serving_names=["child_id"])
    entity_a.save()
    entity_b.save()
    entity_c.save()

    updated_entity_c = await app_container.entity_relationship_service.add_relationship(
        parent=ParentEntity(
            id=entity_a.id, data_id=PydanticObjectId(ObjectId()), data_type=TableDataType.EVENT_DATA
        ),
        child_id=entity_c.id,
    )

    # same entity ID is valid
    validate_entities([entity_a.id], [entity_a.id])

    # parent child relationship is valid
    validate_entities([entity_a.id], [updated_entity_c.id])

    # no parent-child relationship, and not same ID, should throw error
    with pytest.raises(ValueError) as exc:
        validate_entities([entity_a.id], [entity_b.id])
    assert "do not have a parent-child relationship" in str(exc)


@pytest.fixture(name="event_data_id")
def event_data_id_fixture():
    """
    Get event data id
    """
    return PydanticObjectId(ObjectId("6332f9651050ee7d12311111"))


@pytest.fixture(name="tabular_data_id")
def tabular_data_id_fixture():
    """
    Get tabular_data_id
    """
    return PydanticObjectId(ObjectId("6332f9651050ee7d12322222"))


@pytest.fixture(name="get_dataframe_with_type")
def dataframe_fixture(global_graph, snowflake_feature_store, event_data_id, tabular_data_id):
    """
    Frame test fixture
    """

    def get_data_frame_with_type(
        table_data_type: TableDataType, data_id_to_use: Optional[PydanticObjectId] = tabular_data_id
    ):
        columns_info = [
            {"name": "CUST_ID", "dtype": DBVarType.INT},
        ]
        table_details = {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "transaction",
        }
        node = global_graph.add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "id": data_id_to_use,
                "type": table_data_type,
                "columns": columns_info,
                "table_details": table_details,
                "feature_store_details": {
                    "type": "snowflake",
                    "details": {
                        "database": "db",
                        "sf_schema": "public",
                        "account": "account",
                        "warehouse": "warehouse",
                    },
                },
                "event_data_id": event_data_id,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        return Frame(
            feature_store=snowflake_feature_store,
            tabular_source={
                "feature_store_id": snowflake_feature_store.id,
                "table_details": table_details,
            },
            columns_info=columns_info,
            node_name=node.name,
        )

    return get_data_frame_with_type


def test_series_data_type(get_dataframe_with_type):
    """
    Test _series_data_type
    """
    data_type = TableDataType.ITEM_DATA
    item_df = get_dataframe_with_type(data_type)
    item_series = item_df["CUST_ID"]
    series_data_type = _series_data_type(item_series)
    assert series_data_type == data_type


def test_series_tabular_data_id(get_dataframe_with_type, tabular_data_id):
    """
    Test _series_tabular_data_id
    """
    data_type = TableDataType.ITEM_DATA
    item_df = get_dataframe_with_type(data_type)
    item_series = item_df["CUST_ID"]
    series_data_id = _series_tabular_data_id(item_series)
    assert series_data_id == tabular_data_id


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


def test_is_from_same_data(get_dataframe_with_type):
    """
    Test _is_from_same_data
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]
    assert not _is_from_same_data(item_series, event_series)
    assert _is_from_same_data(item_series, item_series)


def test_both_are_lookup_features(get_dataframe_with_type, snowflake_dimension_view_with_entity):
    """
    Test _both_are_lookup_features
    """
    # Create a lookup feature
    feature = snowflake_dimension_view_with_entity["col_float"].as_feature("FloatFeature")
    assert _both_are_lookup_features(feature, feature)

    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    assert not _both_are_lookup_features(item_series, item_series)


def test_get_event_and_item_data_series(get_dataframe_with_type):
    """
    Test _get_event_and_item_data_series
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]
    output_item_series, output_event_series = _get_event_and_item_data_series(
        item_series, event_series
    )
    assert output_item_series == item_series
    assert output_event_series == event_series

    output_item_series, output_event_series = _get_event_and_item_data_series(
        event_series, item_series
    )
    assert output_item_series == item_series
    assert output_event_series == event_series


def test_is_one_item_and_one_event(get_dataframe_with_type):
    """
    Test _is_one_item_and_one_event
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]
    assert _is_one_item_and_one_event(item_series, event_series)
    assert _is_one_item_and_one_event(event_series, item_series)
    assert not _is_one_item_and_one_event(item_series, item_series)


def test_get_event_data_id_of_item_series__no_error(get_dataframe_with_type, event_data_id):
    """
    Test _get_event_data_id_of_item_series
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    actual_event_data_id = _get_event_data_id_of_item_series(item_series)
    assert actual_event_data_id == event_data_id


def test_get_event_data_id_of_item_series__error(get_dataframe_with_type):
    """
    Test _get_event_data_id_of_item_series - error if series is not item data
    """
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]
    with pytest.raises(ValueError) as exc:
        _get_event_data_id_of_item_series(event_series)
    assert "cannot find event data ID from series" in str(exc)


def test_item_data_and_event_data_are_related(get_dataframe_with_type, event_data_id):
    """
    Test _item_data_and_event_data_are_related
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]

    # series both of item_data type should not be related here
    assert not _item_data_and_event_data_are_related(item_series, item_series)

    # one series from item data, and one from event data, where the item_data_id matches event_data's id
    # should be related
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA, event_data_id)
    event_series = event_df["CUST_ID"]
    assert _item_data_and_event_data_are_related(item_series, event_series)


def test_validate_feature_type(get_dataframe_with_type, event_data_id):
    """
    Test _validate_feature_type
    """
    item_df = get_dataframe_with_type(TableDataType.ITEM_DATA)
    item_series = item_df["CUST_ID"]
    event_df = get_dataframe_with_type(TableDataType.EVENT_DATA)
    event_series = event_df["CUST_ID"]

    with pytest.raises(ValueError) as exc:
        validate_feature_type(item_series, event_series)
    assert "features are not of the right type" in str(exc)

    event_df_with_matching_id = get_dataframe_with_type(TableDataType.EVENT_DATA, event_data_id)
    event_series_with_matching_id = event_df_with_matching_id["CUST_ID"]
    validate_feature_type(item_series, event_series_with_matching_id)
