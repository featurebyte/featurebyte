"""
Unit test for DataColumn class
"""
import textwrap

import pytest

from featurebyte.api.base_table import TableColumn
from featurebyte.api.entity import Entity
from featurebyte.api.event_view import EventView
from featurebyte.exception import RecordRetrievalException
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.cleaning_operation import (
    MissingValueImputation,
    StringValueImputation,
    ValueBeyondEndpointImputation,
)


def test_data_column__as_entity(snowflake_event_data, mock_api_object_cache):
    """Test setting a column in the event data as entity"""
    _ = mock_api_object_cache

    # check no column associate with any entity
    assert all([col.entity_id is None for col in snowflake_event_data.columns_info])

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    col_int = snowflake_event_data.col_int
    assert isinstance(col_int, TableColumn)
    snowflake_event_data.col_int.as_entity("customer")

    # check event data column's info attribute & event data's columns_info
    assert snowflake_event_data.col_int.info.entity_id == entity.id
    for col in snowflake_event_data.columns_info:
        if col.name == "col_int":
            assert col.entity_id == entity.id

    with pytest.raises(TypeError) as exc:
        snowflake_event_data.col_int.as_entity(1234)
    assert 'type of argument "entity_name" must be one of (str, NoneType); got int instead' in str(
        exc.value
    )

    with pytest.raises(RecordRetrievalException) as exc:
        snowflake_event_data.col_int.as_entity("some_random_entity")
    expected_msg = (
        'Entity (name: "some_random_entity") not found. Please save the Entity object first.'
    )
    assert expected_msg in str(exc.value)

    # remove entity association
    snowflake_event_data.col_int.as_entity(None)
    assert snowflake_event_data.col_int.info.entity_id is None


def test_data_column__as_entity__saved_data(saved_event_data, config, mock_api_object_cache):
    """Test setting a column in the event data as entity (saved event data)"""
    _ = mock_api_object_cache

    # check no column associate with any entity
    assert all([col.entity_id is None for col in saved_event_data.columns_info])

    # create entity
    entity = Entity(name="customer", serving_names=["cust_id"])
    entity.save()

    saved_event_data.col_int.as_entity("customer")
    assert saved_event_data.saved is True

    # check event data column's info attribute & event data's columns_info
    has_col_int_column = False
    assert saved_event_data.col_int.info.entity_id == entity.id
    for col in saved_event_data.columns_info:
        if col.name == "col_int":
            assert col.entity_id == entity.id
            has_col_int_column = True
    assert has_col_int_column, "columns_info does not contain col_int"

    # check that the column entity map is saved to persistent
    client = config.get_client()
    response = client.get(url=f"/event_data/{saved_event_data.id}")
    response_dict = response.json()
    has_col_int_column = False
    for col in response_dict["columns_info"]:
        if col["name"] == "col_int":
            assert col["entity_id"] == str(entity.id)
            has_col_int_column = True
    assert has_col_int_column, "columns_info does not contain col_int"


def test_data_column__as_entity__saved__entity_not_found_exception(saved_event_data, config):
    """Test setting a column in the event data as entity (record retrieve exception)"""
    # test unexpected exception
    with pytest.raises(RecordRetrievalException) as exc:
        saved_event_data.col_int.as_entity("random_entity")

    expected = 'Entity (name: "random_entity") not found. Please save the Entity object first.'
    assert expected in str(exc)


def _check_event_data_with_critical_data_info(event_data):
    """ "Check update critical data info"""
    # check that event data node type is INPUT when there's no critical data info
    assert event_data.frame.node.type == NodeType.INPUT

    # update critical data info with empty cleaning operation list
    assert event_data.col_boolean.info.critical_data_info is None
    event_data.col_boolean.update_critical_data_info(cleaning_operations=[])
    assert event_data.col_boolean.info.dict() == {
        "name": "col_boolean",
        "dtype": "BOOL",
        "entity_id": None,
        "semantic_id": None,
        "critical_data_info": {"cleaning_operations": []},
    }
    assert event_data.frame.node.type == NodeType.INPUT

    # update critical data info with some cleaning operations
    event_data.col_int.update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0),
            ValueBeyondEndpointImputation(type="less_than", end_point=0, imputed_value=0),
        ]
    )
    event_data.col_float.update_critical_data_info(
        cleaning_operations=[StringValueImputation(imputed_value=0.0)]
    )

    assert event_data.frame.node.type == NodeType.INPUT
    expected_clean_data_query = textwrap.dedent(
        """
        SELECT
          CAST(CASE
            WHEN (
              CAST(CASE WHEN (
                "col_int" IS NULL
              ) THEN 0 ELSE "col_int" END AS BIGINT) < 0
            )
            THEN 0
            ELSE CAST(CASE WHEN (
              "col_int" IS NULL
            ) THEN 0 ELSE "col_int" END AS BIGINT)
          END AS BIGINT) AS "col_int",
          CAST(CASE WHEN IS_VARCHAR(TO_VARIANT("col_float")) THEN 0 ELSE "col_float" END AS FLOAT) AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          CAST("event_timestamp" AS STRING) AS "event_timestamp",
          CAST("created_at" AS STRING) AS "created_at",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
    """
    ).strip()
    assert event_data.preview_clean_data_sql() == expected_clean_data_query

    expected_view_query = textwrap.dedent(
        """
        SELECT
          CAST(CASE
            WHEN (
              CAST(CASE WHEN (
                "col_int" IS NULL
              ) THEN 0 ELSE "col_int" END AS BIGINT) < 0
            )
            THEN 0
            ELSE CAST(CASE WHEN (
              "col_int" IS NULL
            ) THEN 0 ELSE "col_int" END AS BIGINT)
          END AS BIGINT) AS "col_int",
          CAST(CASE WHEN IS_VARCHAR(TO_VARIANT("col_float")) THEN 0 ELSE "col_float" END AS FLOAT) AS "col_float",
          "col_char" AS "col_char",
          "col_text" AS "col_text",
          "col_binary" AS "col_binary",
          "col_boolean" AS "col_boolean",
          CAST("event_timestamp" AS STRING) AS "event_timestamp",
          "cust_id" AS "cust_id"
        FROM "sf_database"."sf_schema"."sf_table"
        LIMIT 10
    """
    ).strip()
    event_view = event_data.get_view()
    assert event_view.preview_sql() == expected_view_query


def _check_remove_critical_data_info(event_data):
    """ "Check remove critical data info"""
    assert event_data.frame.node.type == NodeType.INPUT
    event_data.col_boolean.update_critical_data_info(cleaning_operations=[])
    event_data.col_int.update_critical_data_info(cleaning_operations=[])
    event_data.col_float.update_critical_data_info(cleaning_operations=[])
    for column_info in event_data.columns_info:
        if column_info.critical_data_info:
            assert not column_info.critical_data_info.cleaning_operations
        else:
            assert column_info.critical_data_info is None

    assert event_data.frame.node.type == NodeType.INPUT
    event_view = event_data.get_view()
    assert event_view.node.type == NodeType.GRAPH
    assert event_view.node.parameters.graph.edges == [
        {"source": "proxy_input_1", "target": "project_1"}
    ]


def test_data_column__update_critical_data_info(snowflake_event_data, mock_api_object_cache):
    """Test update critical data info of a data column"""
    _ = mock_api_object_cache
    _check_event_data_with_critical_data_info(snowflake_event_data)
    _check_remove_critical_data_info(snowflake_event_data)


def test_data_column__update_critical_data_info__saved_data(
    saved_event_data, mock_api_object_cache
):
    """Test update critical data info of a saved data column"""
    _ = mock_api_object_cache
    _check_event_data_with_critical_data_info(saved_event_data)
    _check_remove_critical_data_info(saved_event_data)
