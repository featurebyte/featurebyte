"""
Test BaseDataDocumentService subclasses
"""
import pytest

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError
from featurebyte.schema.event_data import EventDataCreate
from featurebyte.schema.item_data import ItemDataCreate


@pytest.mark.asyncio
async def test_data_document_services__retrieval(
    event_data_service, item_data_service, event_data, item_data
):
    """Test data document services retrieval"""
    retrieved_event_data = await event_data_service.get_document(document_id=event_data.id)
    assert retrieved_event_data == event_data

    retrieved_item_data = await item_data_service.get_document(document_id=item_data.id)
    assert retrieved_item_data == item_data

    # check retrieval
    event_data_docs = await event_data_service.list_documents(page_size=0)
    event_data_ids = {doc["_id"] for doc in event_data_docs["data"]}

    item_data_docs = await item_data_service.list_documents(page_size=0)
    item_data_ids = {doc["_id"] for doc in item_data_docs["data"]}

    assert event_data.id in event_data_ids
    assert item_data.id in item_data_ids
    assert event_data.id not in item_data_ids
    assert item_data.id not in event_data_ids

    # check event data & item data are logically mutually exclusive
    with pytest.raises(DocumentNotFoundError) as exc:
        await event_data_service.get_document(document_id=item_data.id)
    msg = f'EventData (id: "{item_data.id}") not found. Please save the EventData object first.'
    assert msg in str(exc.value)

    with pytest.raises(DocumentNotFoundError) as exc:
        await item_data_service.get_document(document_id=event_data.id)
    expected_msg = (
        f'ItemData (id: "{event_data.id}") not found. Please save the ItemData object first.'
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_data_document_services__creation_conflict(
    event_data_service, item_data_service, event_data, item_data
):
    """Test data document services - document creation conflict"""
    with pytest.raises(DocumentConflictError) as exc:
        event_data_dict = event_data.dict(by_alias=True)
        event_data_dict["_id"] = item_data.id
        await event_data_service.create_document(data=EventDataCreate(**event_data_dict))

    expected_msg = (
        f'TabularData (id: "{item_data.id}") already exists. '
        f'Get the existing object by `ItemData.get(name="{item_data.name}")`.'
    )
    assert expected_msg in str(exc.value)

    with pytest.raises(DocumentConflictError) as exc:
        item_data_dict = item_data.dict(by_alias=True)
        item_data_dict["_id"] = event_data.id
        await item_data_service.create_document(data=ItemDataCreate(**item_data_dict))

    expected_msg = (
        f'TabularData (id: "{event_data.id}") already exists. '
        f'Get the existing object by `EventData.get(name="{event_data.name}")`.'
    )
    assert expected_msg in str(exc.value)
