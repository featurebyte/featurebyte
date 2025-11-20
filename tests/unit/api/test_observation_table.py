"""
Unit tests for ObservationTable class
"""

from typing import Any, Dict
from unittest.mock import call, patch

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte import TargetNamespace
from featurebyte.api.observation_table import ObservationTable
from featurebyte.enum import DBVarType, TargetType
from featurebyte.exception import RecordCreationException
from featurebyte.models.observation_table import Purpose
from tests.unit.api.base_materialize_table_test import BaseMaterializedTableApiTest


class TestObservationTable(BaseMaterializedTableApiTest):
    """
    Test observation table
    """

    table_type = ObservationTable

    def assert_info_dict(self, info_dict: Dict[str, Any]) -> None:
        assert info_dict["table_details"]["table_name"].startswith("OBSERVATION_TABLE_")
        assert info_dict == {
            "name": "observation_table_from_source_table",
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": info_dict["table_details"]["table_name"],
            },
            "created_at": info_dict["created_at"],
            "updated_at": None,
            "description": None,
            "target_name": None,
        }

    @pytest.mark.skip(reason="use other test due to testing of more fixtures")
    def test_list(self, table_under_test): ...


@pytest.mark.usefixtures("observation_table_from_source", "observation_table_from_view")
def test_list(catalog):
    """
    Test listing ObservationTable objects
    """
    _ = catalog
    df = ObservationTable.list()
    assert df.columns.tolist() == [
        "id",
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    assert df["name"].tolist() == [
        "observation_table_from_event_view",
        "observation_table_from_source_table",
    ]
    assert (df["feature_store_name"] == "sf_featurestore").all()
    assert df["type"].tolist() == ["view", "source_table"]
    assert df["shape"].tolist() == [[100, 2]] * 2


def test_shape(observation_table_from_source):
    """
    Test shape method
    """
    assert observation_table_from_source.shape() == (100, 2)


def test_data_source(observation_table_from_source):
    """
    Test the underlying SourceTable is constructed properly
    """
    observation_table = observation_table_from_source
    source_table = observation_table._source_table
    assert source_table.feature_store.id == observation_table.location.feature_store_id
    assert source_table.tabular_source.table_details == observation_table.location.table_details


@patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_preview(mock_execute_query, observation_table_from_source):
    """
    Test preview() calls the underlying SourceTable's preview() method
    """

    def side_effect(query, **kwargs):
        _ = query, kwargs
        return pd.DataFrame()

    mock_execute_query.side_effect = side_effect
    observation_table_from_source.preview(limit=123)
    assert len(mock_execute_query.call_args_list) == 1

    sql_query = mock_execute_query.call_args_list[0][0][0]
    assert mock_execute_query.call_args_list[0]

    # check generated SQL query
    assert sql_query.startswith('SELECT\n  *\nFROM "sf_database"."sf_schema"."OBSERVATION_TABLE_')
    assert sql_query.endswith('ORDER BY\n  "__FB_TABLE_ROW_INDEX"\nLIMIT 123')


def test_sample(observation_table_from_source, mock_source_table):
    """
    Test sample() calls the underlying SourceTable's sample() method
    """
    result = observation_table_from_source.sample(size=123, seed=456)
    assert mock_source_table.sample.call_args == call(size=123, seed=456)
    assert result is mock_source_table.sample.return_value


def test_describe(observation_table_from_source, mock_source_table):
    """
    Test describe() calls the underlying SourceTable's describe() method
    """
    result = observation_table_from_source.describe(size=123, seed=456)
    assert mock_source_table.describe.call_args == call(size=123, seed=456)
    assert result is mock_source_table.describe.return_value


def test_update_purpose(observation_table_from_source):
    """
    Test update purpose
    """
    assert observation_table_from_source.purpose is None
    observation_table_from_source.update_purpose(purpose=Purpose.EDA)
    assert observation_table_from_source.purpose == Purpose.EDA

    purpose_in_str = Purpose.TRAINING.value
    assert isinstance(purpose_in_str, str)
    observation_table_from_source.update_purpose(purpose=purpose_in_str)
    assert observation_table_from_source.purpose == Purpose.TRAINING


def test_create_observation_table_without_primary_entity(snowflake_event_table):
    """Test create observation table without primary entity"""
    view = snowflake_event_table.get_view()
    expected_error = (
        "No primary entities found. Please specify the primary entities when "
        "creating the observation table."
    )
    with pytest.raises(ValueError, match=expected_error):
        view.create_observation_table(
            "my_observation_table_from_event_view",
            sample_rows=100,
            columns=["event_timestamp", "cust_id"],
            columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        )


def test_entity_related_properties(observation_table_from_view, cust_id_entity, transaction_entity):
    """Test entity related properties"""
    # FIXME: column_info's entity_id should be set, and entity_ids should not be empty
    assert observation_table_from_view.entity_ids == []
    assert set(observation_table_from_view.primary_entity_ids) == {
        cust_id_entity.id,
        transaction_entity.id,
    }
    primary_entity = observation_table_from_view.primary_entity
    assert len(primary_entity) == 2
    assert {entity.name for entity in primary_entity} == {"customer", "transaction"}
    assert observation_table_from_view.entities == []
    assert observation_table_from_view.target_namespace is None
    assert observation_table_from_view.target is None


def test_create_observation_table_with_target_column_from_view(snowflake_event_view_with_entity):
    """Test create observation table with target column"""
    expected_error = "Target name not found: target"
    with pytest.raises(RecordCreationException, match=expected_error):
        return snowflake_event_view_with_entity.create_observation_table(
            "observation_table_from_event_view",
            columns_rename_mapping={
                "col_int": "transaction_id",
                "event_timestamp": "POINT_IN_TIME",
            },
            target_column="target",
        )


def test_create_observation_table_with_target_column_from_source_table(
    catalog, cust_id_entity, patched_observation_table_service, snowflake_database_table
):
    """Test create observation table with target column"""
    _ = catalog
    _ = patched_observation_table_service

    target_namespace = TargetNamespace.create(
        "target", primary_entity=[cust_id_entity.name], dtype=DBVarType.FLOAT
    )
    observation_table = snowflake_database_table.create_observation_table(
        "observation_table_from_source_table",
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME", "col_float": "target"},
        target_column="target",
        primary_entities=[cust_id_entity.name],
    )
    assert observation_table.target_namespace == target_namespace
    assert observation_table.target is None


@patch("featurebyte.service.target_helper.compute_target.TargetComputer.compute")
def test_create_observation_table_with_target_definition(
    mock_compute, observation_table_from_view, float_target
):
    """Test create observation table with target"""
    mock_compute.return_value.is_output_view = False
    float_target.save()
    observation_table = float_target.compute_target_table(
        observation_table=observation_table_from_view,
        observation_table_name="observation_table_with_target_definition",
    )
    assert observation_table.target_namespace == float_target.target_namespace
    assert observation_table.target == float_target


def test_create_observation_table_with_context(
    snowflake_database_table,
    patched_observation_table_service,
    snowflake_execute_query_for_observation_table,
    catalog,
    context,
):
    """Test create observation table with use case"""
    _ = catalog
    _ = patched_observation_table_service
    observation_table = snowflake_database_table.create_observation_table(
        "my_observation_table_with_use_case",
        sample_rows=100,
        columns=["event_timestamp", "cust_id"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        context_name=context.name,
    )
    # check that context is set correctly
    assert observation_table.context_id == context.id
    # check that primary entity IDs are populated from the context
    assert observation_table.primary_entity_ids == [ObjectId("63f94ed6ea1f050131379214")]


@patch("featurebyte.session.session_helper.validate_output_row_index")
def test_create_observation_table_with_use_case(
    mock_validate_output_row_index,
    snowflake_database_table,
    patched_observation_table_service,
    snowflake_execute_query_for_observation_table,
    catalog,
    use_case,
):
    """Test create observation table with use case"""
    _ = mock_validate_output_row_index
    _ = catalog
    _ = patched_observation_table_service
    observation_table = snowflake_database_table.create_observation_table(
        "my_observation_table_with_use_case",
        sample_rows=100,
        columns=["event_timestamp", "cust_id"],
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        use_case_name=use_case.name,
    )
    # check that user case is set correctly
    assert observation_table.use_case_ids[0] == use_case.id
    # check that context is set correctly
    assert observation_table.context_id == use_case.context_id
    # check that primary entity IDs are populated from the context
    assert observation_table.primary_entity_ids == [ObjectId("63f94ed6ea1f050131379214")]

    mock_validate_materialized_table_and_get_metadata = patched_observation_table_service
    call_args_list = mock_validate_materialized_table_and_get_metadata.call_args_list
    assert len(call_args_list) == 2

    # ensure first call to validate_materialized_table_and_get_metadata does not include target_namespace_id
    first_call_kwargs = call_args_list[0].kwargs
    assert call_args_list[0][0][1].table_name == "__TEMP_OBSERVATION_TABLE_000000000000000000000000"
    assert "target_namespace_id" not in first_call_kwargs

    # ensure second call to validate_materialized_table_and_get_metadata includes target_namespace_id
    second_call_kwargs = call_args_list[1].kwargs
    assert call_args_list[1][0][1].table_name == "OBSERVATION_TABLE_000000000000000000000000"
    assert "target_namespace_id" in second_call_kwargs


def test_create_observation_table_from_observation_table(
    catalog, cust_id_entity, patched_observation_table_service, snowflake_database_table
):
    """Test create observation table from another observation table"""
    _ = catalog
    _ = patched_observation_table_service

    target_namespace = TargetNamespace.create(
        "target", primary_entity=[cust_id_entity.name], dtype=DBVarType.INT
    )
    target_namespace.update_target_type(TargetType.CLASSIFICATION)
    observation_table = snowflake_database_table.create_observation_table(
        "observation_table_from_source_table",
        columns_rename_mapping={"event_timestamp": "POINT_IN_TIME", "col_int": "target"},
        target_column="target",
        primary_entities=[cust_id_entity.name],
    )
    assert observation_table.target_namespace == target_namespace
    assert observation_table.target is None

    new_observation_table = observation_table.create_observation_table(
        "new_observation_table",
    )
    assert new_observation_table.target_namespace == observation_table.target_namespace
    assert new_observation_table.target == observation_table.target
    assert new_observation_table.context_id == observation_table.context_id
    assert new_observation_table.use_case_ids == observation_table.use_case_ids
    assert new_observation_table.primary_entity_ids == observation_table.primary_entity_ids
