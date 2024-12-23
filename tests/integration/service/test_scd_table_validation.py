"""
Integration tests for SCDTableValidationService
"""

import pandas as pd
import pytest

from featurebyte.enum import DBVarType
from featurebyte.exception import TableValidationError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.scd_table_validation import SCDTableValidationService


@pytest.fixture
def service(app_container) -> SCDTableValidationService:
    """
    Fixture for SCDTableValidationService
    """
    return app_container.scd_table_validation_service


@pytest.fixture
def document_service(app_container) -> SCDTableService:
    """
    Fixture for SCDTableService
    """
    return app_container.scd_table_service


@pytest.fixture
def scd_create_payload(feature_store, session_without_datasets, table_name):
    """
    Fixture for SCDTableCreate payload
    """
    return SCDTableCreate(
        name=table_name,
        tabular_source=TabularSource(
            feature_store_id=feature_store.id,
            table_details=TableDetails(
                database_name=session_without_datasets.database_name,
                schema_name=session_without_datasets.schema_name,
                table_name=table_name,
            ),
        ),
        columns_info=[
            ColumnSpecWithDescription(
                name="effective_ts",
                dtype=DBVarType.TIMESTAMP,
            ),
            ColumnSpecWithDescription(
                name="cust_id",
                dtype=DBVarType.INT,
            ),
            ColumnSpecWithDescription(
                name="value",
                dtype=DBVarType.INT,
            ),
        ],
        natural_key_column="cust_id",
        effective_timestamp_column="effective_ts",
    )


@pytest.fixture
def scd_create_with_end_date_payload(scd_create_payload):
    """
    Fixture for SCDTableCreate payload with end_timestamp_column
    """
    scd_create_payload.columns_info.append(
        ColumnSpecWithDescription(
            name="end_ts",
            dtype=DBVarType.TIMESTAMP,
        )
    )
    scd_create_payload.end_timestamp_column = "end_ts"
    return scd_create_payload


@pytest.mark.parametrize("table_name", ["test_validate_scd_table__valid"])
@pytest.mark.asyncio
async def test_validate_scd_table__valid(
    service,
    document_service,
    scd_create_payload,
    session_without_datasets,
    table_name,
):
    """
    Test validate SCD table (valid case)
    """
    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-04-12 10:00:00",
            "2022-04-12 10:00:00",
            "2022-04-20 10:00:00",
            "2022-04-20 10:00:00",
        ]),
        "cust_id": [1000, 1001, 1000, 1001],
        "value": [1, 1, 2, 2],
    })
    await session.register_table(table_name, df_scd)
    table_model = await document_service.create_document(scd_create_payload)
    await service._validate_table(session, table_model)


@pytest.mark.parametrize("table_name", ["test_validate_scd_table__invalid_multiple_active_records"])
@pytest.mark.asyncio
async def test_validate_scd_table__invalid_multiple_active_records(
    service,
    document_service,
    scd_create_with_end_date_payload,
    session_without_datasets,
    table_name,
):
    """
    Test validate SCD table (invalid case)
    """
    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-04-12 10:00:00",
            "2022-04-14 10:00:00",
            "2022-04-20 10:00:00",
        ]),
        "end_ts": pd.to_datetime([
            "2050-04-12 10:00:00",
            "2050-04-14 10:00:00",
            "2022-04-25 10:00:00",
        ]),
        "cust_id": [1000, 1000, 1001],
        "value": [1, 1, 2],
    })
    await session.register_table(table_name, df_scd)
    table_model = await document_service.create_document(scd_create_with_end_date_payload)
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(session, table_model)
    assert (
        str(exc_info.value)
        == "Multiple active records found for the same natural key. Examples of natural keys with multiple active records are: [1000]"
    )


@pytest.mark.parametrize(
    "table_name", ["test_validate_scd_table__invalid_multiple_records_per_ts_id"]
)
@pytest.mark.asyncio
async def test_validate_scd_table__invalid_multiple_records_per_ts_id(
    service,
    document_service,
    scd_create_payload,
    session_without_datasets,
    table_name,
):
    """
    Test validate SCD table (invalid case)
    """
    session = session_without_datasets
    df_scd = pd.DataFrame({
        "effective_ts": pd.to_datetime([
            "2022-04-12 10:00:00",
            "2022-04-12 10:00:00",
            "2024-06-12 10:00:00",
        ]),
        "cust_id": [1000, 1000, 1000],
        "value": [1, 2, 3],
    })
    await session.register_table(table_name, df_scd)
    table_model = await document_service.create_document(scd_create_payload)
    with pytest.raises(TableValidationError) as exc_info:
        await service._validate_table(session, table_model)
    assert (
        str(exc_info.value)
        == "Multiple records found for the same effective timestamp and natural key combination. Examples of invalid natural keys: [1000]"
    )
