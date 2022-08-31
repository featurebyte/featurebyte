"""
EventDataService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.entity import EntityModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.common.operation import DictProject, DictTransform
from featurebyte.service.feature_store import FeatureStoreService


class EventDataService(BaseDocumentService[EventDataModel]):
    """
    EventDataService class
    """

    document_class = EventDataModel
    info_transform = DictTransform(
        rule={
            **BaseDocumentService.base_info_transform_rule,
            "__root__": DictProject(
                rule=[
                    "event_timestamp_column",
                    "record_creation_date_column",
                ]
            ),
            "columns": DictProject(
                rule=("columns_info", ["name", "var_type", "entity"]), verbose_only=True
            ),
        }
    )
    foreign_key_map = {"entity_id": EntityModel.collection_name()}

    async def create_document(
        self, data: EventDataCreate, get_credential: Any = None
    ) -> EventDataModel:
        _ = get_credential
        _ = await FeatureStoreService(user=self.user, persistent=self.persistent).get_document(
            document_id=data.tabular_source.feature_store_id
        )
        document = EventDataModel(
            user_id=self.user.id, status=EventDataStatus.DRAFT, **data.json_dict()
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(self, document_id: ObjectId, data: EventDataUpdate) -> EventDataModel:
        document = await self.get_document(document_id=document_id)

        # prepare update payload
        update_payload = data.dict()

        # check eligibility of status transition
        eligible_transitions = {
            EventDataStatus.DRAFT: {EventDataStatus.PUBLISHED},
            EventDataStatus.PUBLISHED: {EventDataStatus.DEPRECATED},
            EventDataStatus.DEPRECATED: {},
        }
        current_status = document.status
        if (
            current_status != data.status
            and data.status not in eligible_transitions[current_status]
        ):
            raise DocumentUpdateError(
                f"Invalid status transition from {current_status} to {data.status}."
            )

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": update_payload},
            user_id=self.user.id,
        )
        return await self.get_document(document_id=document_id)
