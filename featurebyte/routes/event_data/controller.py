"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.event_data import (
    EventDataModel,
    EventDataStatus,
    FeatureJobSettingHistoryEntry,
)
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController, GetType
from featurebyte.routes.common.util import get_utc_now
from featurebyte.schema.event_data import EventDataCreate, EventDataList, EventDataUpdate


class EventDataController(BaseController[EventDataModel, EventDataList]):
    """
    EventData controller
    """

    collection_name = CollectionName.EVENT_DATA
    document_class = EventDataModel
    paginated_document_class = EventDataList

    @classmethod
    async def create_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        data: EventDataCreate,
    ) -> EventDataModel:
        """
        Create Event Data at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: EventDataCreate
            EventData creation payload

        Returns
        -------
        EventDataModel
            Newly created event data object
        """
        # exclude microseconds from timestamp as it's not supported in persistent
        utc_now = get_utc_now()

        # check the existence of the feature store at persistent
        feature_store_id, _ = data.tabular_source
        _ = await cls.get_document(
            user=user,
            persistent=persistent,
            collection_name=CollectionName.FEATURE_STORE,
            document_id=feature_store_id,
        )

        # init history and set status to draft
        if data.default_feature_job_setting:
            history = [
                FeatureJobSettingHistoryEntry(
                    created_at=utc_now,
                    setting=data.default_feature_job_setting,
                )
            ]
        else:
            history = []

        # check any conflict with existing documents
        constraints_check_triples: list[tuple[dict[str, Any], dict[str, Any], GetType]] = [
            ({"_id": data.id}, {"id": data.id}, "name"),
            ({"name": data.name}, {"name": data.name}, "name"),
        ]
        for query_filter, doc_represent, get_type in constraints_check_triples:
            await cls.check_document_creation_conflict(
                persistent=persistent,
                query_filter=query_filter,
                doc_represent=doc_represent,
                get_type=get_type,
                user_id=user.id,
            )

        document = EventDataModel(
            user_id=user.id,
            status=EventDataStatus.DRAFT,
            history=history,
            **data.json_dict(),
        )
        insert_id = await persistent.insert_one(
            collection_name=cls.collection_name,
            document=document.dict(by_alias=True),
            user_id=user.id,
        )
        assert insert_id == document.id == data.id

        return await cls.get(user=user, persistent=persistent, document_id=insert_id)

    @classmethod
    async def update_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        event_data_id: ObjectId,
        data: EventDataUpdate,
    ) -> EventDataModel:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        event_data_id: ObjectId
            EventData ID
        data: EventDataUpdate
            Event data update payload

        Returns
        -------
        EventDataModel
            EventData object with updated attribute(s)

        Raises
        ------
        HTTPException
            Invalid event data status transition
        """
        query_filter = {"_id": ObjectId(event_data_id), "user_id": user.id}
        event_data = (
            await cls.get(user=user, persistent=persistent, document_id=event_data_id)
        ).dict(by_alias=True)

        # prepare update payload
        update_payload = data.dict()
        if data.default_feature_job_setting:
            update_payload["history"] = [
                FeatureJobSettingHistoryEntry(
                    created_at=get_utc_now(),
                    setting=update_payload["default_feature_job_setting"],
                ).dict()
            ] + event_data["history"]
        else:
            update_payload.pop("default_feature_job_setting")

        if data.column_entity_map is not None:
            update_payload["column_entity_map"] = data.column_entity_map
        else:
            update_payload.pop("column_entity_map")

        if data.status:
            # check eligibility of status transition
            eligible_transitions = {
                EventDataStatus.DRAFT: {EventDataStatus.PUBLISHED},
                EventDataStatus.PUBLISHED: {EventDataStatus.DEPRECATED},
                EventDataStatus.DEPRECATED: {},
            }
            current_status = event_data["status"]
            if data.status not in eligible_transitions[current_status]:
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=f"Invalid status transition from {current_status} to {data.status}.",
                )
        else:
            update_payload.pop("status")

        await persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": update_payload},
            user_id=user.id,
        )

        return await cls.get(user=user, persistent=persistent, document_id=event_data_id)
