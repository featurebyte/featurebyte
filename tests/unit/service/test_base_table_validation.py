"""
Unit tests for BaseTableValidationService
"""

from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import TimestampSchema
from featurebyte.enum import DBVarType, SourceType
from featurebyte.exception import TableValidationError
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.model.column_info import ColumnInfo, ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import FeatureStoreDetails, SnowflakeDetails, TableDetails
from featurebyte.query_graph.sql.materialisation import ExtendedSourceMetadata
from featurebyte.query_graph.sql.source_info import SourceInfo
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
    updated_table_model_dict = updated_table_model.dict()
    assert updated_table_model_dict["validation"].pop("updated_at") is not None
    assert updated_table_model_dict["validation"] == {
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
    updated_table_model_dict = updated_table_model.dict()
    assert updated_table_model_dict["validation"].pop("updated_at") is not None
    assert updated_table_model_dict["validation"] == {
        "status": "FAILED",
        "validation_message": "custom message",
        "task_id": None,
    }


@pytest.fixture
def timestamp_schema():
    """TimestampSchema fixture"""
    return TimestampSchema(format_string="%Y-%m-%d %H:%M:%S", timezone="Etc/UTC")


@pytest.fixture(name="mock_metadata")
def mock_metadata_fixture() -> ExtendedSourceMetadata:
    """
    Fixture for mock ExtendedSourceMetadata
    """
    return ExtendedSourceMetadata(
        columns_info=[
            ColumnInfo(
                name="effective_timestamp",
                dtype=DBVarType.TIMESTAMP,
                entity_id=None,
                semantic_id=None,
            ),
            ColumnInfo(name="cust_id", dtype=DBVarType.INT, entity_id=None, semantic_id=None),
            ColumnInfo(
                name="end_timestamp", dtype=DBVarType.TIMESTAMP, entity_id=None, semantic_id=None
            ),
        ],
        feature_store_id=ObjectId("65f8b5e01234567890abcdef"),
        feature_store_details=FeatureStoreDetails(
            type=SourceType.SNOWFLAKE,
            details=SnowflakeDetails(
                account="sf_account",
                database_name="my_db",
                schema_name="my_schema",
                warehouse="sf_warehouse",
                role_name="TESTING",
            ),
        ),
        source_info=SourceInfo(
            database_name="my_db", schema_name="my_schema", source_type=SourceType.SNOWFLAKE
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params,expected_table_needs_validation,expected_call",
    [
        # ({}, False, 0),
        ({"effective_timestamp_column": "effective_timestamp"}, True, 1),
        ({"end_timestamp_column": "end_timestamp"}, True, 1),
        (
            {
                "effective_timestamp_column": "effective_timestamp",
                "end_timestamp_column": "end_timestamp",
            },
            True,
            2,
        ),
    ],
)
async def test_scd_table_validation(
    app_container,
    scd_table,
    params,
    expected_table_needs_validation,
    timestamp_schema,
    mock_metadata,
    expected_call,
):
    """
    Test SCDTableValidationService
    """
    params["natural_key_column"] = None
    columns_info = scd_table.columns_info
    for param in ["effective_timestamp", "end_timestamp"]:
        if params.get(f"{param}_column"):
            for column in columns_info:
                if column.name == params[f"{param}_column"]:
                    column.dtype = DBVarType.VARCHAR
            params[f"{param}_schema"] = timestamp_schema

    table_model = SCDTableModel(**{
        **scd_table.model_dump(by_alias=True),
        **params,
        "columns_info": columns_info,
    })
    scd_table_validation_service = app_container.scd_table_validation_service
    table_needs_validation = scd_table_validation_service.table_needs_validation(table_model)
    assert table_needs_validation == expected_table_needs_validation

    with patch.object(
        scd_table_validation_service, "_validate_timestamp_format_string"
    ) as mock_validate_timestamp_format_string:
        await scd_table_validation_service.validate_table(
            session=Mock(), table_model=table_model, metadata=mock_metadata
        )
        assert mock_validate_timestamp_format_string.call_count == expected_call
