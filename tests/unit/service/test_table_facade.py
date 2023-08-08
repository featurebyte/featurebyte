"""Test table facade service"""
import pytest

from featurebyte import ColumnCleaningOperation, MissingValueImputation


@pytest.mark.asyncio
async def test_update_table_column_cleaning_operations(app_container, event_table):
    """Test update_table_column_cleaning_operations"""
    await app_container.table_facade_service.update_table_column_cleaning_operations(
        table_id=event_table.id,
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="col_int", cleaning_operations=[MissingValueImputation(imputed_value=0)]
            )
        ],
    )
    updated_table = await app_container.event_table_service.get_document(event_table.id)
    col_info = next(col_info for col_info in updated_table.columns_info)
    assert col_info.critical_data_info.cleaning_operations == [
        MissingValueImputation(imputed_value=0)
    ]

    # remove cleaning operations
    await app_container.table_facade_service.update_table_column_cleaning_operations(
        table_id=event_table.id,
        column_cleaning_operations=[
            ColumnCleaningOperation(column_name="col_int", cleaning_operations=[])
        ],
    )
    updated_table = await app_container.event_table_service.get_document(event_table.id)
    col_info = next(col_info for col_info in updated_table.columns_info)
    assert col_info.critical_data_info.cleaning_operations == []

    # attempt to update non-existing column
    with pytest.raises(ValueError) as exc:
        await app_container.table_facade_service.update_table_column_cleaning_operations(
            table_id=event_table.id,
            column_cleaning_operations=[
                ColumnCleaningOperation(column_name="non_exist_column", cleaning_operations=[])
            ],
        )

    expected_msg = f"Columns ['non_exist_column'] not found in table {event_table.id}"
    assert expected_msg in str(exc.value)
