"""
Integration tests for StaticSourceTable
"""

import pytest

from tests.integration.api.materialized_table.utils import (
    check_location_valid,
    check_materialized_table_accessible,
    check_materialized_table_preview_methods,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
async def test_static_source_table_from_source_table(
    data_source, feature_store, session, source_type, transaction_data_upper_case_expected
):
    """
    Test creating a static source table from a source table
    """
    source_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name="TEST_TABLE",
    )
    sample_rows = 123
    static_source_table = source_table.create_static_source_table(
        f"MY_STATIC_SOURCE_TABLE_{source_type}", sample_rows=sample_rows
    )
    assert static_source_table.name == f"MY_STATIC_SOURCE_TABLE_{source_type}"
    table_details = static_source_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)
    check_materialized_table_preview_methods(
        static_source_table, expected_columns=transaction_data_upper_case_expected.columns.tolist()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
async def test_static_source_table_from_view(event_table, scd_table, session, source_type):
    """
    Test creating an static source table from a view
    """
    view = event_table.get_view()
    scd_view = scd_table.get_view()
    view = view.join(scd_view, on="ÜSER ID")
    sample_rows = 123
    static_source_table = view.create_static_source_table(
        f"MY_STATIC_SOURCE_TABLE_FROM_VIEW_{source_type}",
        sample_rows=sample_rows,
        columns=[view.timestamp_column, "ÜSER ID"],
        columns_rename_mapping={view.timestamp_column: "TS"},
    )
    assert static_source_table.name == f"MY_STATIC_SOURCE_TABLE_FROM_VIEW_{source_type}"
    table_details = static_source_table.location.table_details
    check_location_valid(table_details, session)
    await check_materialized_table_accessible(table_details, session, source_type, sample_rows)
    check_materialized_table_preview_methods(
        static_source_table,
        expected_columns=["TS", "ÜSER ID"],
    )
