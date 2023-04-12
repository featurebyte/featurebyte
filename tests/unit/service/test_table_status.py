"""
Test TableStatusService
"""
import pytest

from featurebyte.exception import DocumentConflictError, DocumentUpdateError
from featurebyte.models.feature_store import TableStatus
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "from_status,to_status,is_valid",
    [
        (TableStatus.PUBLIC_DRAFT, TableStatus.PUBLIC_DRAFT, True),
        (TableStatus.PUBLIC_DRAFT, TableStatus.PUBLISHED, True),
        (TableStatus.PUBLISHED, TableStatus.DEPRECATED, True),
        (TableStatus.PUBLIC_DRAFT, TableStatus.DEPRECATED, True),
        (TableStatus.PUBLISHED, TableStatus.PUBLIC_DRAFT, False),
        (TableStatus.DEPRECATED, TableStatus.PUBLIC_DRAFT, False),
        (TableStatus.DEPRECATED, TableStatus.PUBLISHED, False),
    ],
)
async def test_update_table_status(
    table_status_service, event_table_service, event_table, from_status, to_status, is_valid
):
    """Test update_table_status"""
    # setup event table status for testing
    await event_table_service.update_document(
        document_id=event_table.id, data=EventTableServiceUpdate(status=from_status)
    )
    doc = await event_table_service.get_document(document_id=event_table.id)
    assert doc.status == from_status
    if is_valid:
        await table_status_service.update_status(
            service=event_table_service,
            document_id=event_table.id,
            status=to_status,
        )
        doc = await event_table_service.get_document(document_id=event_table.id)
        assert doc.status == (to_status or from_status)
    else:
        with pytest.raises(DocumentUpdateError) as exc:
            await table_status_service.update_status(
                service=event_table_service,
                document_id=event_table.id,
                status=to_status,
            )
        assert f"Invalid status transition from {from_status} to {to_status}" in str(exc.value)


@pytest.mark.asyncio
async def test_tabular_source_uniqueness_check_excludes_deprecated_tables(
    event_table, event_table_service, table_status_service
):
    """Test tabular source uniqueness check excludes deprecated tables"""
    assert event_table.status == TableStatus.PUBLIC_DRAFT
    new_table_payload = EventTableCreate(
        **event_table.dict(exclude={"_id": True, "name": True}), name="new_table"
    )
    with pytest.raises(DocumentConflictError) as exc:
        await event_table_service.create_document(data=new_table_payload)
    expected_msg = (
        'already exists. Get the existing object by `EventTable.get(name="sf_event_table")`.'
    )
    assert expected_msg in str(exc.value)

    # deprecate the existing event table
    await event_table_service.update_document(
        document_id=event_table.id, data=EventTableServiceUpdate(status=TableStatus.DEPRECATED)
    )
    doc = await event_table_service.get_document(document_id=event_table.id)
    assert doc.status == TableStatus.DEPRECATED

    # create a new table with the same tabular source
    new_event_table = await event_table_service.create_document(data=new_table_payload)
    assert new_event_table.name == "new_table"
    assert new_event_table.status == TableStatus.PUBLIC_DRAFT
    assert new_event_table.id != event_table.id
