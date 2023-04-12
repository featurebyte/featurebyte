"""
Test TableStatusService
"""
import pytest

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_store import TableStatus
from featurebyte.schema.event_table import EventTableServiceUpdate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "from_status,to_status,is_valid",
    [
        (TableStatus.PUBLIC_DRAFT, TableStatus.PUBLIC_DRAFT, True),
        (TableStatus.PUBLIC_DRAFT, TableStatus.PUBLISHED, True),
        (TableStatus.PUBLISHED, TableStatus.DEPRECATED, True),
        (TableStatus.PUBLIC_DRAFT, TableStatus.DEPRECATED, False),
        (TableStatus.PUBLISHED, TableStatus.PUBLIC_DRAFT, False),
        (TableStatus.DEPRECATED, TableStatus.PUBLIC_DRAFT, False),
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
