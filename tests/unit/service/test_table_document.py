"""
Test BaseTableDocumentService subclasses
"""
import pytest
from bson import ObjectId

from featurebyte.exception import (
    DocumentConflictError,
    DocumentModificationBlockedError,
    DocumentNotFoundError,
)
from featurebyte.models.base import ReferenceInfo
from featurebyte.schema.event_table import EventTableCreate
from featurebyte.schema.item_table import ItemTableCreate


@pytest.mark.asyncio
async def test_table_document_services__retrieval(
    event_table_service, item_table_service, event_table, item_table
):
    """Test table document services retrieval"""
    retrieved_event_table = await event_table_service.get_document(document_id=event_table.id)
    assert retrieved_event_table == event_table

    retrieved_item_table = await item_table_service.get_document(document_id=item_table.id)
    assert retrieved_item_table == item_table

    # check retrieval
    event_table_docs = await event_table_service.list_documents_as_dict(page_size=0)
    event_table_ids = {doc["_id"] for doc in event_table_docs["data"]}

    item_table_docs = await item_table_service.list_documents_as_dict(page_size=0)
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
async def test_table_document_services__creation_conflict(
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


@pytest.mark.asyncio
async def test_table_document_service__update_column_description(
    scd_table_service, scd_table, app_container, entity
):
    """Test table document service - update column description"""
    # add block_by_modification
    reference_info = ReferenceInfo(asset_name="Asset", document_id=ObjectId())
    await scd_table_service.add_block_modification_by(
        query_filter={"_id": scd_table.id},
        reference_info=reference_info,
    )

    # update column description
    columns_info = scd_table.columns_info
    columns_info[0].description = "new description"
    await app_container.table_facade_service.update_table_columns_info(
        table_id=scd_table.id,
        columns_info=columns_info,
        service=scd_table_service,
        skip_block_modification_check=True,
    )

    # check column description is updated
    retrieved_event_table = await scd_table_service.get_document(document_id=scd_table.id)
    assert retrieved_event_table.columns_info[0].description == "new description"

    # attempt to update other column info (should fail)
    columns_info[0].entity_id = entity.id
    with pytest.raises(DocumentModificationBlockedError):
        await app_container.table_facade_service.update_table_columns_info(
            table_id=scd_table.id,
            columns_info=columns_info,
            service=scd_table_service,
        )
