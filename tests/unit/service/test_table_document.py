"""
Test BaseTableDocumentService subclasses
"""
import pytest

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.schema.event_table import EventTableCreate
from featurebyte.schema.item_table import ItemTableCreate


@pytest.mark.asyncio
async def test_data_document_services__retrieval(
    event_table_service, item_table_service, event_table, item_table
):
    """Test table document services retrieval"""
    retrieved_event_table = await event_table_service.get_document(document_id=event_table.id)
    assert retrieved_event_table == event_table

    retrieved_item_table = await item_table_service.get_document(document_id=item_table.id)
    assert retrieved_item_table == item_table

    # check retrieval
    event_table_docs = await event_table_service.list_documents(page_size=0)
    event_table_ids = {doc["_id"] for doc in event_table_docs["data"]}

    item_table_docs = await item_table_service.list_documents(page_size=0)
    item_table_ids = {doc["_id"] for doc in item_table_docs["data"]}

    assert event_table.id in event_table_ids
    assert item_table.id in item_table_ids
    assert event_table.id not in item_table_ids
    assert item_table.id not in event_table_ids

    # check event table & item table are logically mutually exclusive
    with pytest.raises(DocumentNotFoundError) as exc:
        await event_table_service.get_document(document_id=item_table.id)
    msg = f'EventTable (id: "{item_table.id}") not found. Please save the EventTable object first.'
    assert msg in str(exc.value)

    with pytest.raises(DocumentNotFoundError) as exc:
        await item_table_service.get_document(document_id=event_table.id)
    expected_msg = (
        f'ItemTable (id: "{event_table.id}") not found. Please save the ItemTable object first.'
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_data_document_services__creation_conflict(
    event_table_service, item_table_service, event_table, item_table
):
    """Test table document services - document creation conflict"""
    with pytest.raises(DocumentConflictError) as exc:
        event_table_dict = event_table.dict(by_alias=True)
        event_table_dict["_id"] = item_table.id
        await event_table_service.create_document(data=EventTableCreate(**event_table_dict))

    expected_msg = (
        f'Table (id: "{item_table.id}") already exists. '
        f'Get the existing object by `ItemTable.get(name="{item_table.name}")`.'
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(DocumentConflictError) as exc:
        item_table_dict = item_table.dict(by_alias=True)
        item_table_dict["_id"] = event_table.id
        await item_table_service.create_document(data=ItemTableCreate(**item_table_dict))

    expected_msg = (
        f'Table (id: "{event_table.id}") already exists. '
        f'Get the existing object by `EventTable.get(name="{event_table.name}")`.'
    )
    assert expected_msg in str(exc.value)
