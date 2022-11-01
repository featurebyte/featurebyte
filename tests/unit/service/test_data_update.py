"""
Test DataUpdateService
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.event_data import EventDataUpdate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "from_status,to_status,is_valid",
    [
        (DataStatus.DRAFT, None, True),
        (DataStatus.DRAFT, DataStatus.DRAFT, True),
        (DataStatus.DRAFT, DataStatus.PUBLISHED, True),
        (DataStatus.PUBLISHED, DataStatus.DEPRECATED, True),
        (DataStatus.DRAFT, DataStatus.DEPRECATED, False),
        (DataStatus.PUBLISHED, DataStatus.DRAFT, False),
        (DataStatus.DEPRECATED, DataStatus.DRAFT, False),
    ],
)
async def test_update_data_status(
    data_update_service, event_data_service, event_data, from_status, to_status, is_valid
):
    """Test update_data_status"""
    # setup event data status for testing
    await event_data_service.update_document(
        document_id=event_data.id, data=EventDataUpdate(status=from_status)
    )
    doc = await event_data_service.get_document(document_id=event_data.id)
    assert doc.status == from_status
    if is_valid:
        await data_update_service.update_data_status(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataUpdate(status=to_status),
        )
        doc = await event_data_service.get_document(document_id=event_data.id)
        assert doc.status == (to_status or from_status)
    else:
        with pytest.raises(DocumentUpdateError) as exc:
            await data_update_service.update_data_status(
                service=event_data_service,
                document_id=event_data.id,
                data=EventDataUpdate(status=to_status),
            )
        assert f"Invalid status transition from {from_status} to {to_status}" in str(exc.value)


@pytest.mark.asyncio
async def test_update_columns_info(
    data_update_service, event_data_service, entity_service, semantic_service, event_data, entity
):
    """Test update_columns_info"""
    _ = entity
    new_entity = await entity_service.create_document(
        data=EntityCreate(name="an_entity", serving_name="a_name")
    )
    new_semantic = await semantic_service.get_or_create_document(name="a_semantic")
    columns_info = event_data.dict()["columns_info"]
    columns_info[0]["entity_id"] = new_entity.id
    columns_info[0]["semantic_id"] = new_semantic.id

    # update columns info
    await data_update_service.update_columns_info(
        service=event_data_service,
        document_id=event_data.id,
        data=EventDataUpdate(columns_info=columns_info),
    )

    # check the updated column info
    updated_doc = await event_data_service.get_document(document_id=event_data.id)
    assert updated_doc.columns_info[0].entity_id == new_entity.id
    assert updated_doc.columns_info[0].semantic_id == new_semantic.id

    # test unknown entity ID
    unknown_id = ObjectId()
    columns_info[0]["entity_id"] = unknown_id
    with pytest.raises(DocumentUpdateError) as exc:
        await data_update_service.update_columns_info(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataUpdate(columns_info=columns_info),
        )

    expected_msg = f"Entity IDs ['{unknown_id}'] not found for columns ['col_int']"
    assert expected_msg in str(exc.value)

    # test unknown semantic ID
    columns_info[0]["entity_id"] = None
    columns_info[0]["semantic_id"] = unknown_id
    with pytest.raises(DocumentUpdateError) as exc:
        await data_update_service.update_columns_info(
            service=event_data_service,
            document_id=event_data.id,
            data=EventDataUpdate(columns_info=columns_info),
        )

    expected_msg = f"Semantic IDs ['{unknown_id}'] not found for columns ['col_int']"
    assert expected_msg in str(exc.value)
