"""
Test observation table
"""

from collections import OrderedDict
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from featurebyte.enum import DBVarType, InternalName, SpecialColumnName
from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.materialized_table import BaseMaterializedTableService
from featurebyte.worker.task.observation_table import ObservationTableTask


@pytest.mark.asyncio
async def test_get_task_description(catalog, app_container):
    """
    Test get task description
    """
    payload = ObservationTableTaskPayload(
        name="Test Observation Table",
        feature_store_id=ObjectId(),
        catalog_id=catalog.id,
        request_input=SourceTableObservationInput(
            source=TabularSource(
                feature_store_id=ObjectId(),
                table_details=TableDetails(table_name="test_table"),
            ),
        ),
        primary_entity_ids=["63f94ed6ea1f050131379214"],
    )
    task = app_container.get(ObservationTableTask)
    assert (
        await task.get_task_description(payload)
        == 'Save observation table "Test Observation Table" from source table.'
    )


@pytest.mark.asyncio
async def test_rename_observation_weight_column_present():
    """Test rename_observation_weight_column when OBSERVATION_WEIGHT column exists"""
    mock_session = AsyncMock()
    mock_session.list_table_schema.return_value = OrderedDict({
        "POINT_IN_TIME": ColumnSpec(name="POINT_IN_TIME", dtype=DBVarType.TIMESTAMP),
        "cust_id": ColumnSpec(name="cust_id", dtype=DBVarType.VARCHAR),
        SpecialColumnName.OBSERVATION_WEIGHT: ColumnSpec(
            name=SpecialColumnName.OBSERVATION_WEIGHT, dtype=DBVarType.FLOAT
        ),
    })

    table_details = TableDetails(table_name="test_table")
    result = await BaseMaterializedTableService.rename_observation_weight_column(
        session=mock_session, table_details=table_details
    )

    assert result is True
    mock_session.create_table_as.assert_called_once()
    call_args = mock_session.create_table_as.call_args
    # create_table_as(table_details, select_expr, replace=True)
    select_expr = call_args.args[1]
    sql = select_expr.sql()
    assert InternalName.TABLE_ROW_WEIGHT in sql
    assert SpecialColumnName.OBSERVATION_WEIGHT in sql


@pytest.mark.asyncio
async def test_rename_observation_weight_column_absent():
    """Test rename_observation_weight_column when OBSERVATION_WEIGHT column does not exist"""
    mock_session = AsyncMock()
    mock_session.list_table_schema.return_value = OrderedDict({
        "POINT_IN_TIME": ColumnSpec(name="POINT_IN_TIME", dtype=DBVarType.TIMESTAMP),
        "cust_id": ColumnSpec(name="cust_id", dtype=DBVarType.VARCHAR),
    })

    table_details = TableDetails(table_name="test_table")
    result = await BaseMaterializedTableService.rename_observation_weight_column(
        session=mock_session, table_details=table_details
    )

    assert result is False
    mock_session.create_table_as.assert_not_called()
