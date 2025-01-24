"""Test table facade service"""

import pytest

from featurebyte import ColumnCleaningOperation, MissingValueImputation


@pytest.mark.asyncio
async def test_update_table_column_cleaning_operations(app_container, snowflake_event_table):
    """Test update_table_column_cleaning_operations"""
    event_table = snowflake_event_table
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


@pytest.mark.asyncio
async def test_update_table_default_feature_job_setting(
    app_container,
    snowflake_scd_table,
    snowflake_time_series_table,
    snowflake_item_table,
    arbitrary_default_feature_job_setting,
    arbitrary_default_cron_feature_job_setting,
):
    """Test update_table_column_cleaning_operations"""
    table_facade_service = app_container.table_facade_service
    assert snowflake_scd_table.default_feature_job_setting != arbitrary_default_feature_job_setting
    await table_facade_service.update_default_feature_job_setting(
        table_id=snowflake_scd_table.id,
        default_feature_job_setting=arbitrary_default_feature_job_setting,
    )
    updated_table = await app_container.table_service.get_document(
        document_id=snowflake_scd_table.id
    )
    assert updated_table.default_feature_job_setting == arbitrary_default_feature_job_setting

    assert (
        snowflake_time_series_table.default_feature_job_setting
        != arbitrary_default_cron_feature_job_setting
    )
    await table_facade_service.update_default_feature_job_setting(
        table_id=snowflake_time_series_table.id,
        default_feature_job_setting=arbitrary_default_cron_feature_job_setting,
    )
    updated_table = await app_container.table_service.get_document(
        document_id=snowflake_time_series_table.id
    )
    assert updated_table.default_feature_job_setting == arbitrary_default_cron_feature_job_setting

    with pytest.raises(ValueError) as exc:
        await table_facade_service.update_default_feature_job_setting(
            table_id=snowflake_item_table.id,
            default_feature_job_setting=arbitrary_default_feature_job_setting,
        )
        assert "Default feature job setting not supported for table type" in str(exc.value)
