"""
Unit tests for BaseTableValidationService
"""

from unittest.mock import patch

import pytest
import pytest_asyncio

from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.scd_table import SCDTableCreate


@pytest.fixture(name="document_service")
def document_service_fixture(app_container):
    """Fixture for DocumentService"""
    return app_container.scd_table_service


@pytest.fixture(name="table_validation_service")
def table_validation_service_fixture(app_container):
    """TableValidationService fixture. Use SCDBaseTableValidationService for testing"""
    with patch(
        "featurebyte.service.base_table_validation.SessionManagerService.get_feature_store_session"
    ):
        yield app_container.scd_table_validation_service


@pytest_asyncio.fixture(name="table_model")
async def table_model_fixture(document_service, feature_store):
    """
    Table model fixture
    """
    payload = SCDTableCreate(
        name="my_scd_table",
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name="my_db",
                schema_name="my_schema",
                table_name="my_table",
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="effective_date",
                dtype=DBVarType.TIMESTAMP,
            ),
            ColumnSpecWithDescription(
                name="cust_id",
                dtype=DBVarType.INT,
            ),
        ],
        natural_key_column="cust_id",
        effective_timestamp_column="effective_date",
    )
    return await document_service.create_document(payload)


@pytest.mark.asyncio
async def test_validate_and_update__success(
    table_validation_service,
    document_service,
    table_model,
):
    """
    Test validate_and_update (success case)
    """
    with patch.object(table_validation_service, "validate_table", side_effect=None):
        await table_validation_service.validate_and_update(table_model.id)
    updated_table_model = await document_service.get_document(table_model.id)
    assert updated_table_model.validation.dict() == {
        "status": "PASSED",
        "validation_message": None,
        "task_id": None,
    }


@pytest.mark.asyncio
async def test_validate_and_update__failure(
    table_validation_service,
    document_service,
    table_model,
):
    """
    Test validate_and_update (failure case)
    """
    with patch.object(
        table_validation_service,
        "validate_table",
        side_effect=TableValidationError("custom message"),
    ):
        await table_validation_service.validate_and_update(table_model.id)
    updated_table_model = await document_service.get_document(table_model.id)
    assert updated_table_model.validation.dict() == {
        "status": "FAILED",
        "validation_message": "custom message",
        "task_id": None,
    }
